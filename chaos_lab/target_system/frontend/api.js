// api.js — shared API client module pointing to API Gateway on port 8010

const API_BASE = window.API_URL || 'http://localhost:8010';

export const getToken = () => localStorage.getItem('shopcore_token');
export const getUserId = () => localStorage.getItem('shopcore_user_id');
export const isLoggedIn = () => !!getToken();

export async function api(path, options = {}) {
  const headers = { 'Content-Type': 'application/json', ...options.headers };
  const token = getToken();
  if (token) headers['Authorization'] = `Bearer ${token}`;

  const res = await fetch(`${API_BASE}${path}`, { ...options, headers });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: 'Request failed' }));
    throw new Error(err.detail || `HTTP ${res.status}`);
  }
  return res.json();
}

export const products = {
  list:    (cat) => api(`/products${cat ? '?category='+cat : ''}`),
  get:     (id)  => api(`/products/${id}`),
};

export const orders = {
  create:  (data) => api('/orders', { method: 'POST', body: JSON.stringify(data) }),
  list:    (uid)  => api(`/orders?user_id=${uid}`),
};

export const cart = {
  get:     (uid)          => api(`/cart/${uid}`),
  add:     (uid, pid, qty) => api(`/cart/${uid}/items`, {
                               method: 'POST',
                               body: JSON.stringify({ product_id: parseInt(pid), quantity: parseInt(qty) })
                             }),
  remove:  (uid, pid)     => api(`/cart/${uid}/items/${pid}`, { method: 'DELETE' }),
  clear:   (uid)          => api(`/cart/${uid}`, { method: 'DELETE' }),
};

export const auth = {
  login:    (data) => api('/users/login',    { method: 'POST', body: JSON.stringify(data) }),
  register: (data) => api('/users/register', { method: 'POST', body: JSON.stringify(data) }),
  me:       ()     => api('/users/me'),
  logout:   async () => {
    try {
      await api('/users/logout', { method: 'POST' });
    } catch (e) {
      console.warn("Logout endpoint error:", e);
    } finally {
      localStorage.removeItem('shopcore_token');
      localStorage.removeItem('shopcore_user_id');
      localStorage.removeItem('shopcore_user_name');
      location.href = 'auth.html';
    }
  }
};

// Update header auth links and cart count
export async function updateHeaderUI() {
  const authLink = document.getElementById('auth-link');
  if (authLink) {
    if (isLoggedIn()) {
      const userName = localStorage.getItem('shopcore_user_name') || 'User';
      authLink.innerHTML = `Logout (${userName})`;
      authLink.href = '#';
      authLink.onclick = (e) => {
        e.preventDefault();
        auth.logout();
      };
    } else {
      authLink.textContent = 'Login';
      authLink.href = 'auth.html';
      authLink.onclick = null;
    }
  }

  const cartCount = document.getElementById('cart-count');
  if (cartCount) {
    const uid = getUserId();
    if (uid) {
      try {
        const c = await cart.get(uid);
        let totalQty = 0;
        if (c && c.items) {
          for (const pid in c.items) {
            totalQty += c.items[pid];
          }
        }
        cartCount.textContent = totalQty;
      } catch (e) {
        console.warn("Failed to update cart count:", e);
        cartCount.textContent = '0';
      }
    } else {
      cartCount.textContent = '0';
    }
  }
}
