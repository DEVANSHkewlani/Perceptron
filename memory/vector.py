"""
Vector Memory — Qdrant
======================
Vector storage and semantic search for episodic memory events.
Uses local sentence-transformers for offline, fast embedding generation.
"""
from __future__ import annotations
import asyncio
import json
import logging
import uuid
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
log = logging.getLogger("memory.vector")

class VectorMemory:
    def __init__(self, qdrant_url: str, collection_name: str = "cognitive_events"):
        self.qdrant_url = qdrant_url
        self.collection_name = collection_name
        self._client: AsyncQdrantClient | None = None
        self._model = None
        self.dimension = 384
        self._lock: asyncio.Lock | None = None

    async def connect(self):
        self._client = AsyncQdrantClient(url=self.qdrant_url, timeout=30.0)
        self._lock = asyncio.Lock()

        # Check and create collection
        try:
            exists = await self._client.collection_exists(self.collection_name)
            if not exists:
                log.info(f"Creating Qdrant collection '{self.collection_name}' with 384 dimensions...")
                await self._client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(size=self.dimension, distance=Distance.COSINE),
                )
                log.info(f"Qdrant collection '{self.collection_name}' created successfully")
        except Exception as e:
            if "already exists" in str(e) or "Conflict" in str(e):
                log.info(f"Qdrant collection '{self.collection_name}' already exists (concurrency handled)")
            else:
                raise

        # Load local embedding model in a background task to prevent blocking server startup
        asyncio.create_task(self._load_model_bg())

    async def _load_model_bg(self):
        try:
            log.info("Loading local SentenceTransformer model 'all-MiniLM-L6-v2' in background...")
            def load():
                from sentence_transformers import SentenceTransformer
                return SentenceTransformer("all-MiniLM-L6-v2")
            self._model = await asyncio.to_thread(load)
            log.info("SentenceTransformer model loaded successfully in background")
        except Exception as e:
            log.error(f"Failed to load SentenceTransformer model in background: {e}")

    async def disconnect(self):
        if self._client:
            await self._client.close()
            log.info("Vector memory client closed")

    def _get_qdrant_id(self, event_id: str) -> str:
        try:
            return str(uuid.UUID(event_id))
        except ValueError:
            return str(uuid.uuid5(uuid.NAMESPACE_DNS, event_id))

    async def store(self, event: dict):
        if not self._client or not self._model:
            log.warning("VectorMemory not connected, skipping store")
            return

        event_id = event.get("event_id")
        if not event_id:
            log.warning("Event has no event_id, skipping vector store")
            return

        event_type = event.get("event_type", "unknown")
        source_id = event.get("source_id", "unknown")
        payload = event.get("payload", {})
        
        # Build document text to embed
        text = f"Type: {event_type} | Source: {source_id} | Payload: {json.dumps(payload)}"
        
        # Run embedding in executor thread with lock to prevent concurrency issues
        async with self._lock:
            embeddings = await asyncio.to_thread(self._model.encode, [text])
        vector = embeddings[0].tolist()

        qdrant_id = self._get_qdrant_id(event_id)
        
        # Save event dict in payload
        await self._client.upsert(
            collection_name=self.collection_name,
            points=[
                PointStruct(
                    id=qdrant_id,
                    vector=vector,
                    payload=event
                )
            ]
        )
        log.info(f"Stored event {event_id} in Qdrant collection {self.collection_name}")

    async def search(self, query_text: str, limit: int = 5) -> list[dict]:
        if not self._client or not self._model:
            log.warning("VectorMemory not connected, returning empty search results")
            return []

        async with self._lock:
            embeddings = await asyncio.to_thread(self._model.encode, [query_text])
        vector = embeddings[0].tolist()

        res = await self._client.query_points(
            collection_name=self.collection_name,
            query=vector,
            limit=limit
        )
        return [p.payload for p in res.points if p.payload]

    async def search_similar_events(self, event_id: str, limit: int = 5) -> list[dict]:
        if not self._client:
            log.warning("VectorMemory not connected, returning empty similar events")
            return []

        qdrant_id = self._get_qdrant_id(event_id)
        try:
            points = await self._client.retrieve(
                collection_name=self.collection_name,
                ids=[qdrant_id],
                with_vectors=True
            )
            if not points:
                log.info(f"No vector point found for event_id: {event_id}")
                return []
            
            # Extract vector (Qdrant retrieve returns points with vector property)
            vector = points[0].vector
            res = await self._client.query_points(
                collection_name=self.collection_name,
                query=vector,
                limit=limit + 1
            )
            
            # Exclude the query event itself from the results
            return [p.payload for p in res.points if p.payload and p.payload.get("event_id") != event_id][:limit]
        except Exception as e:
            log.error(f"Failed to search similar events for {event_id}: {e}")
            return []
