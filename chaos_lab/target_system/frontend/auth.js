import { auth } from './api.js';

function switchTab(active, hide) {
  const activeTab = document.getElementById(active);
  const hideTab = document.getElementById(hide);
  if (activeTab) activeTab.classList.add('active');
  if (hideTab) hideTab.classList.remove('active');
  
  const loginForm = document.getElementById('login-form');
  const registerForm = document.getElementById('register-form');
  
  if (loginForm) loginForm.classList.toggle('hidden', active !== 'tab-login');
  if (registerForm) registerForm.classList.toggle('hidden', active !== 'tab-register');
}

const tabLogin = document.getElementById('tab-login');
const tabRegister = document.getElementById('tab-register');

if (tabLogin) tabLogin.onclick = () => switchTab('tab-login', 'tab-register');
if (tabRegister) tabRegister.onclick = () => switchTab('tab-register', 'tab-login');

const loginForm = document.getElementById('login-form');
if (loginForm) {
  loginForm.addEventListener('submit', async e => {
    e.preventDefault();
    const fd = new FormData(e.target);
    const errEl = document.getElementById('login-error');
    if (errEl) errEl.classList.add('hidden');
    
    try {
      const data = await auth.login({ email: fd.get('email'), password: fd.get('password') });
      localStorage.setItem('shopcore_token',     data.token);
      localStorage.setItem('shopcore_user_id',   data.user_id);
      localStorage.setItem('shopcore_user_name', data.name || 'User');
      location.href = 'index.html';
    } catch (err) {
      if (errEl) {
        errEl.textContent = err.message;
        errEl.classList.remove('hidden');
      }
    }
  });
}

const registerForm = document.getElementById('register-form');
if (registerForm) {
  registerForm.addEventListener('submit', async e => {
    e.preventDefault();
    const fd = new FormData(e.target);
    const errEl = document.getElementById('reg-error');
    if (errEl) errEl.classList.add('hidden');
    
    try {
      const data = await auth.register({
        email: fd.get('email'), password: fd.get('password'), name: fd.get('name')
      });
      localStorage.setItem('shopcore_token',     data.token);
      localStorage.setItem('shopcore_user_id',   data.user.id);
      localStorage.setItem('shopcore_user_name', data.user.name || 'User');
      location.href = 'index.html';
    } catch (err) {
      if (errEl) {
        errEl.textContent = err.message;
        errEl.classList.remove('hidden');
      }
    }
  });
}
