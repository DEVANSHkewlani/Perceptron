"""
PlaybookUpdater — updates procedural memory playbook success rates.
"""
from __future__ import annotations

import logging
import httpx

log = logging.getLogger("feedback.playbook")


class PlaybookUpdater:
    def __init__(self, memory_url: str):
        self._url = memory_url

    async def update(self, action: str, outcome: str) -> None:
        """
        Find all playbooks that recommend this action and update their stats.
        success_rate = success_count / (success_count + failure_count)
        """
        success = outcome == "success"

        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                r = await client.get(
                    f"{self._url}/memory/procedural/playbooks",
                    params={"recommended_action": action},
                )
                if r.status_code != 200:
                    log.warning(f"[PlaybookUpdater] GET playbooks failed with status {r.status_code}")
                    return

                playbooks = r.json()
                if not playbooks:
                    log.debug(f"[PlaybookUpdater] No playbooks for action={action}")
                    return

                for pb in playbooks:
                    pb_id = pb.get("id")
                    s_cnt = pb.get("success_count" ) or 0
                    f_cnt = pb.get("failure_count") or 0

                    if success:
                        s_cnt += 1
                    else:
                        f_cnt += 1

                    total = s_cnt + f_cnt
                    new_rate = round(s_cnt / total, 4) if total > 0 else 0.5

                    patch_resp = await client.patch(
                        f"{self._url}/memory/procedural/playbooks/{pb_id}",
                        json={
                            "success_count": s_cnt,
                            "failure_count": f_cnt,
                            "success_rate":  new_rate,
                        },
                    )
                    if patch_resp.status_code == 200:
                        log.info(
                            f"[PlaybookUpdater] pb={pb_id} action={action} "
                            f"outcome={outcome} rate={new_rate:.3f} ({s_cnt}W/{f_cnt}L)"
                        )
                    else:
                        log.warning(
                            f"[PlaybookUpdater] PATCH failed for pb={pb_id} with status {patch_resp.status_code}"
                        )
            except Exception as e:
                import traceback
                log.error(f"[PlaybookUpdater] Error updating playbook stats: {type(e)} {e}")
                traceback.print_exc()
