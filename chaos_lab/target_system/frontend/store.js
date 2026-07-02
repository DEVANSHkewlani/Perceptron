import { products, cart, orders, getUserId, updateHeaderUI } from './api.js';

const CATEGORY_EMOJI = {
  electronics: '💻', peripherals: '🖱️', storage: '💾', accessories: '💡'
};

function productCard(p) {
  return `
    <div class="product-card" data-id="${p.id}">
      <div class="product-image-container">
        <span>${CATEGORY_EMOJI[p.category] || '📦'}</span>
      </div>
      <div class="product-body">
        <div class="product-name"><a href="product.html?id=${p.id}">${p.name}</a></div>
        <div class="product-cat">${p.category}</div>
        <div class="product-footer">
          <span class="product-price">$${parseFloat(p.price).toFixed(2)}</span>
          <button class="btn btn-primary add-cart-btn" data-id="${p.id}">Add</button>
        </div>
      </div>
    </div>`;
}

let allProducts = [];

// ── INDEX PAGE (Product Catalog) ──────────────────────────────────────────
export async function renderProducts(category = '') {
  const grid = document.getElementById('product-grid');
  const errBanner = document.getElementById('error-banner');
  try {
    allProducts = await products.list(category);
    grid.innerHTML = allProducts.map(productCard).join('');
    grid.querySelectorAll('.add-cart-btn').forEach(btn => {
      btn.onclick = (e) => {
        e.preventDefault();
        e.stopPropagation();
        addToCart(btn.dataset.id);
      };
    });
  } catch (e) {
    errBanner.textContent = `Error loading products: ${e.message}`;
    errBanner.classList.remove('hidden');
    grid.innerHTML = '';
  }
}

async function addToCart(productId) {
  const uid = getUserId();
  if (!uid) { location.href = 'auth.html'; return; }
  try {
    await cart.add(uid, productId, 1);
    await updateHeaderUI();
  } catch (e) {
    alert(`Cart error: ${e.message}`);
  }
}

export function initFilters() {
  const search = document.getElementById('search');
  if (search) {
    search.oninput = () => {
      const q = search.value.toLowerCase();
      document.querySelectorAll('.product-card').forEach(card => {
        const name = card.querySelector('.product-name').textContent.toLowerCase();
        card.style.display = name.includes(q) ? '' : 'none';
      });
    };
  }
  document.querySelectorAll('.category-tabs .tab').forEach(tab => {
    tab.onclick = () => {
      document.querySelectorAll('.category-tabs .tab').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      renderProducts(tab.dataset.cat);
    };
  });
}

// ── PRODUCT DETAIL PAGE ───────────────────────────────────────────────────
export async function renderProductDetail() {
  const params = new URLSearchParams(window.location.search);
  const id = params.get('id');
  if (!id) { location.href = 'index.html'; return; }

  const errBanner = document.getElementById('error-banner');
  const detailImg = document.getElementById('detail-img');
  const detailTitle = document.getElementById('detail-title');
  const detailCat = document.getElementById('detail-cat');
  const detailPrice = document.getElementById('detail-price');
  const addBtn = document.getElementById('add-detail-btn');
  const qtyInput = document.getElementById('detail-qty');

  try {
    const p = await products.get(id);
    if (detailImg) detailImg.textContent = CATEGORY_EMOJI[p.category] || '📦';
    if (detailTitle) detailTitle.textContent = p.name;
    if (detailCat) detailCat.textContent = p.category;
    if (detailPrice) detailPrice.textContent = `$${parseFloat(p.price).toFixed(2)}`;

    if (addBtn) {
      addBtn.onclick = async () => {
        const uid = getUserId();
        if (!uid) { location.href = 'auth.html'; return; }
        const qty = parseInt(qtyInput ? qtyInput.value : '1') || 1;
        try {
          addBtn.disabled = true;
          await cart.add(uid, p.id, qty);
          await updateHeaderUI();
          alert("Added to cart!");
        } catch (e) {
          alert(`Cart error: ${e.message}`);
        } finally {
          addBtn.disabled = false;
        }
      };
    }
  } catch (e) {
    if (errBanner) {
      errBanner.textContent = `Error loading product: ${e.message}`;
      errBanner.classList.remove('hidden');
    }
  }
}

// ── CART PAGE ─────────────────────────────────────────────────────────────
export async function renderCart() {
  const uid = getUserId();
  if (!uid) { location.href = 'auth.html'; return; }

  const cartList = document.getElementById('cart-list');
  const errBanner = document.getElementById('error-banner');
  const subtotalEl = document.getElementById('cart-subtotal');
  const totalEl = document.getElementById('cart-total');
  const checkoutBtn = document.getElementById('checkout-btn');

  if (!cartList) return;

  try {
    const userCart = await cart.get(uid);
    const itemIds = Object.keys(userCart.items);
    if (itemIds.length === 0) {
      cartList.innerHTML = '<div style="padding:40px; text-align:center; color:var(--text-dark);">Your cart is empty.</div>';
      if (subtotalEl) subtotalEl.textContent = '$0.00';
      if (totalEl) totalEl.textContent = '$0.00';
      if (checkoutBtn) checkoutBtn.disabled = true;
      return;
    }

    if (checkoutBtn) checkoutBtn.disabled = false;

    let subtotal = 0;
    let html = '';

    for (const pid of itemIds) {
      try {
        const p = await products.get(pid);
        const qty = userCart.items[pid];
        const lineTotal = parseFloat(p.price) * qty;
        subtotal += lineTotal;

        html += `
          <div class="cart-item" data-id="${p.id}">
            <div class="cart-item-img">${CATEGORY_EMOJI[p.category] || '📦'}</div>
            <div>
              <div class="cart-item-title">${p.name}</div>
              <div class="cart-item-cat">${p.category}</div>
            </div>
            <div class="qty-control">
              <button class="qty-btn dec-qty" data-id="${p.id}">-</button>
              <span class="qty-val">${qty}</span>
              <button class="qty-btn inc-qty" data-id="${p.id}">+</button>
            </div>
            <div class="cart-item-price">$${lineTotal.toFixed(2)}</div>
            <button class="remove-btn" data-id="${p.id}">Delete</button>
          </div>`;
      } catch (err) {
        console.warn(`Could not load cart product ${pid}:`, err);
      }
    }

    cartList.innerHTML = html;
    if (subtotalEl) subtotalEl.textContent = `$${subtotal.toFixed(2)}`;
    if (totalEl) totalEl.textContent = `$${subtotal.toFixed(2)}`;

    // Wire actions
    cartList.querySelectorAll('.inc-qty').forEach(btn => {
      btn.onclick = async () => {
        const pid = btn.dataset.id;
        const newQty = userCart.items[pid] + 1;
        await cart.add(uid, pid, newQty);
        await renderCart();
        await updateHeaderUI();
      };
    });

    cartList.querySelectorAll('.dec-qty').forEach(btn => {
      btn.onclick = async () => {
        const pid = btn.dataset.id;
        const newQty = userCart.items[pid] - 1;
        if (newQty <= 0) {
          await cart.remove(uid, pid);
        } else {
          await cart.add(uid, pid, newQty);
        }
        await renderCart();
        await updateHeaderUI();
      };
    });

    cartList.querySelectorAll('.remove-btn').forEach(btn => {
      btn.onclick = async () => {
        const pid = btn.dataset.id;
        await cart.remove(uid, pid);
        await renderCart();
        await updateHeaderUI();
      };
    });

    if (checkoutBtn) {
      checkoutBtn.onclick = async () => {
        try {
          checkoutBtn.disabled = true;
          const productIds = [];
          for (const pid in userCart.items) {
            for (let i = 0; i < userCart.items[pid]; i++) {
              productIds.push(parseInt(pid));
            }
          }
          await orders.create({
            user_id: uid,
            product_ids: productIds,
            total_amount: subtotal
          });
          await cart.clear(uid);
          await updateHeaderUI();
          alert("Order placed successfully!");
          location.href = 'orders.html';
        } catch (e) {
          alert(`Checkout error: ${e.message}`);
        } finally {
          checkoutBtn.disabled = false;
        }
      };
    }

  } catch (e) {
    if (errBanner) {
      errBanner.textContent = `Error loading cart: ${e.message}`;
      errBanner.classList.remove('hidden');
    }
  }
}

// ── ORDERS PAGE ───────────────────────────────────────────────────────────
export async function renderOrders() {
  const uid = getUserId();
  if (!uid) { location.href = 'auth.html'; return; }

  const list = document.getElementById('orders-list');
  const errBanner = document.getElementById('error-banner');
  if (!list) return;

  try {
    const userOrders = await orders.list(uid);
    if (userOrders.length === 0) {
      list.innerHTML = '<div style="padding:40px; text-align:center; color:var(--text-dark);">No orders found.</div>';
      return;
    }

    list.innerHTML = userOrders.map(o => {
      const dt = new Date(o.created_at).toLocaleString();
      return `
        <div class="order-card">
          <div class="order-info">
            <div class="order-id">ID: ${o.id}</div>
            <div class="order-date">${dt}</div>
            <div style="font-size:14px; margin-top:6px; color:var(--text-muted);">Items: ${o.product_ids ? o.product_ids.length : 0} product(s)</div>
          </div>
          <div class="order-meta">
            <span class="order-price">$${parseFloat(o.total_amount).toFixed(2)}</span>
            <span class="status-pill success">${o.status}</span>
          </div>
        </div>`;
    }).join('');
  } catch (e) {
    if (errBanner) {
      errBanner.textContent = `Error loading orders: ${e.message}`;
      errBanner.classList.remove('hidden');
    }
  }
}
