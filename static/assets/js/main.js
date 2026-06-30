const modalTriggers = document.querySelectorAll('[data-open-modal]');
const modals = document.querySelectorAll('.modal');
const closeButtons = document.querySelectorAll('[data-close-modal]');
const pmRuntimeConfig = window.pmRuntimeConfig || {};

const themeToggles = document.querySelectorAll('[data-theme-toggle]');
const themeColorMeta = document.querySelector('meta[name="theme-color"]');

const applyTheme = (theme, persist = false) => {
  const normalizedTheme = theme === 'dark' ? 'dark' : 'light';
  document.documentElement.dataset.theme = normalizedTheme;
  document.documentElement.style.colorScheme = normalizedTheme;
  if (themeColorMeta) {
    themeColorMeta.setAttribute('content', normalizedTheme === 'dark' ? '#0f1117' : '#f6f6f2');
  }
  themeToggles.forEach((toggle) => {
    const nextThemeLabel = normalizedTheme === 'dark' ? 'светлую' : 'тёмную';
    toggle.setAttribute('aria-label', `Включить ${nextThemeLabel} тему`);
    toggle.setAttribute('title', `Включить ${nextThemeLabel} тему`);
    toggle.setAttribute('aria-pressed', normalizedTheme === 'dark' ? 'true' : 'false');
  });
  if (persist) {
    try {
      window.localStorage.setItem('pm-theme', normalizedTheme);
    } catch (error) {
      // Ignore storage errors in private or restricted browser modes.
    }
  }
};

applyTheme(document.documentElement.dataset.theme || 'light');

themeToggles.forEach((toggle) => {
  toggle.addEventListener('click', () => {
    const currentTheme = document.documentElement.dataset.theme === 'dark' ? 'dark' : 'light';
    applyTheme(currentTheme === 'dark' ? 'light' : 'dark', true);
  });
});

const runAfterPageLoad = (callback, delay = 0) => {
  const schedule = () => {
    if ('requestIdleCallback' in window) {
      window.requestIdleCallback(() => window.setTimeout(callback, delay), { timeout: 3000 });
      return;
    }
    window.setTimeout(callback, delay);
  };

  if (document.readyState === 'complete') {
    schedule();
    return;
  }

  window.addEventListener('load', schedule, { once: true });
};

const externalScriptPromises = new Map();

const loadExternalScript = (src, attributes = {}) => {
  if (!src) return Promise.reject(new Error('Missing script src'));
  if (externalScriptPromises.has(src)) return externalScriptPromises.get(src);
  const promise = new Promise((resolve, reject) => {
    const existing = document.querySelector(`script[src="${src}"]`);
    if (existing) {
      if (existing.dataset.loaded === 'true') {
        resolve(existing);
        return;
      }
      existing.addEventListener('load', () => resolve(existing), { once: true });
      existing.addEventListener('error', () => reject(new Error(`Failed to load ${src}`)), { once: true });
      return;
    }
    const script = document.createElement('script');
    script.src = src;
    Object.entries(attributes).forEach(([key, value]) => {
      if (value === true) {
        script.setAttribute(key, '');
        return;
      }
      if (value !== false && value !== null && value !== undefined) {
        script.setAttribute(key, String(value));
      }
    });
    script.async = true;
    script.dataset.loaded = 'false';
    script.addEventListener('load', () => {
      script.dataset.loaded = 'true';
      resolve(script);
    }, { once: true });
    script.addEventListener('error', () => reject(new Error(`Failed to load ${src}`)), { once: true });
    document.head.appendChild(script);
  });
  externalScriptPromises.set(src, promise);
  return promise;
};

const initNonCriticalAnalytics = () => {
  const analytics = pmRuntimeConfig.analytics || {};
  const yandexId = String(analytics.yandexMetrikaId || '').trim();
  const gaId = String(analytics.gaMeasurementId || '').trim();

  if (yandexId && typeof window.ym !== 'function') {
    window.ym = window.ym || function () {
      (window.ym.a = window.ym.a || []).push(arguments);
    };
    window.ym.l = Date.now();
    loadExternalScript(`https://mc.yandex.ru/metrika/tag.js?id=${encodeURIComponent(yandexId)}`)
      .then(() => {
        if (typeof window.ym === 'function') {
          window.ym(Number(yandexId), 'init', {
            ssr: true,
            webvisor: true,
            clickmap: true,
            ecommerce: 'dataLayer',
            referrer: document.referrer,
            url: location.href,
            accurateTrackBounce: true,
            trackLinks: true,
          });
        }
      })
      .catch(() => {});
  }

  if (gaId) {
    loadExternalScript(`https://www.googletagmanager.com/gtag/js?id=${encodeURIComponent(gaId)}`)
      .then(() => {
        window.dataLayer = window.dataLayer || [];
        window.gtag = window.gtag || function () { window.dataLayer.push(arguments); };
        window.gtag('js', new Date());
        window.gtag('config', gaId);
      })
      .catch(() => {});
  }
};

const mountTelegramWidget = () => {
  const node = document.querySelector('[data-telegram-widget]');
  if (!node || node.dataset.pmLoaded === 'true') return;
  const login = String(node.dataset.telegramLogin || '').trim();
  if (!login) return;
  node.dataset.pmLoaded = 'true';
  node.classList.remove('tg-login-button--error');
  node.innerHTML = '';
  const script = document.createElement('script');
  script.async = true;
  script.src = 'https://telegram.org/js/telegram-widget.js?22';
  ['telegramLogin', 'size', 'authUrl', 'onauth', 'requestAccess', 'userpic', 'radius'].forEach((key) => {
    const attrKey = `data-${key.replace(/[A-Z]/g, (char) => `-${char.toLowerCase()}`)}`;
    const value = node.dataset[key];
    if (value) script.setAttribute(attrKey, value);
  });
  script.addEventListener('error', () => {
    node.dataset.pmLoaded = 'false';
    node.classList.add('tg-login-button--error');
    node.innerHTML = '<span class="tg-login-button__fallback">Telegram не загрузился. Обновите страницу или используйте email.</span>';
  }, { once: true });
  node.appendChild(script);
};

runAfterPageLoad(initNonCriticalAnalytics, 1500);
mountTelegramWidget();
runAfterPageLoad(mountTelegramWidget, 300);
document.querySelectorAll('.pm-provider-telegram .pm-provider-button').forEach((button) => {
  button.addEventListener('click', () => {
    const node = document.querySelector('[data-telegram-widget]');
    if (node && node.classList.contains('tg-login-button--error')) {
      node.dataset.pmLoaded = 'false';
    }
    mountTelegramWidget();
  });
});

const syncBodyLock = () => {
  const hasOpenModal = Array.from(modals).some((modal) => modal.classList.contains('open'));
  const hasOpenDrawer = document.body.classList.contains('drawer-open');
  document.body.style.overflow = hasOpenModal || hasOpenDrawer ? 'hidden' : '';
};

const setModalPanel = (modal, panel) => {
  const panels = Array.from(modal.querySelectorAll('.modal-panel'));
  if (!panels.length) return;
  const target = panels.find((item) => item.dataset.panel === panel) || panels[0];
  panels.forEach((item) => item.classList.toggle('active', item === target));
};

const openModal = (name, defaultPanel = 'choice') => {
  const modal = document.querySelector(`.modal[data-modal="${name}"]`);
  if (!modal) return;
  setModalPanel(modal, defaultPanel);
  modal.classList.add('open');
  syncBodyLock();
};

const closeModal = (modal) => {
  modal.classList.remove('open');
  syncBodyLock();
};

modalTriggers.forEach((trigger) => {
  trigger.addEventListener('click', () => {
    const name = trigger.getAttribute('data-open-modal');
    const defaultPanel = trigger.getAttribute('data-modal-default') || 'choice';
    openModal(name, defaultPanel);
  });
});

closeButtons.forEach((btn) => {
  btn.addEventListener('click', (event) => {
    const modal = event.target.closest('.modal');
    if (modal) closeModal(modal);
  });
});

modals.forEach((modal) => {
  modal.addEventListener('click', (event) => {
    const panelTrigger = event.target.closest('[data-modal-panel]');
    if (panelTrigger && modal.contains(panelTrigger)) {
      setModalPanel(modal, panelTrigger.getAttribute('data-modal-panel'));
      return;
    }
    if (event.target === modal) closeModal(modal);
  });
});

const mobileDrawer = document.querySelector('[data-mobile-drawer]');
const drawerToggles = document.querySelectorAll('[data-drawer-toggle]');
const drawerCloseButtons = document.querySelectorAll('[data-drawer-close]');

const closeDrawer = () => {
  if (!mobileDrawer) return;
  document.body.classList.remove('drawer-open');
  syncBodyLock();
};

const openDrawer = () => {
  if (!mobileDrawer) return;
  document.body.classList.add('drawer-open');
  syncBodyLock();
};

drawerToggles.forEach((trigger) => {
  trigger.addEventListener('click', () => {
    if (document.body.classList.contains('drawer-open')) {
      closeDrawer();
      return;
    }
    openDrawer();
  });
});

drawerCloseButtons.forEach((trigger) => {
  trigger.addEventListener('click', closeDrawer);
});

if (mobileDrawer) {
  mobileDrawer.querySelectorAll('a').forEach((link) => {
    link.addEventListener('click', closeDrawer);
  });
}

const coursesMenus = document.querySelectorAll('[data-courses-menu]');

const resetMenuPanelPosition = (panel) => {
  if (!panel) return;
  panel.style.removeProperty('--panel-shift-x');
  panel.style.removeProperty('--panel-max-height');
};

const syncMenuPanelPosition = (panel) => {
  if (!panel || panel.hidden) return;
  resetMenuPanelPosition(panel);
  const viewportPadding = window.innerWidth <= 640 ? 12 : 20;
  const initialRect = panel.getBoundingClientRect();
  let shift = 0;

  if (initialRect.right > window.innerWidth - viewportPadding) {
    shift -= initialRect.right - (window.innerWidth - viewportPadding);
  }
  if (initialRect.left + shift < viewportPadding) {
    shift += viewportPadding - (initialRect.left + shift);
  }

  panel.style.setProperty('--panel-shift-x', `${Math.round(shift)}px`);

  const adjustedRect = panel.getBoundingClientRect();
  const availableHeight = Math.max(180, window.innerHeight - adjustedRect.top - viewportPadding);
  panel.style.setProperty('--panel-max-height', `${Math.round(availableHeight)}px`);
};

const closeAllCourseMenus = (excludeMenu = null) => {
  coursesMenus.forEach((menu) => {
    if (menu === excludeMenu) return;
    menu.classList.remove('is-open');
    const itemToggle = menu.querySelector('[data-courses-toggle]');
    const itemPanel = menu.querySelector('[data-courses-panel]');
    if (itemToggle) itemToggle.setAttribute('aria-expanded', 'false');
    if (itemPanel) {
      itemPanel.hidden = true;
      resetMenuPanelPosition(itemPanel);
    }
  });
};

coursesMenus.forEach((menu) => {
  const toggle = menu.querySelector('[data-courses-toggle]');
  const panel = menu.querySelector('[data-courses-panel]');
  if (!toggle || !panel) return;

  const closeMenu = () => {
    menu.classList.remove('is-open');
    toggle.setAttribute('aria-expanded', 'false');
    panel.hidden = true;
    resetMenuPanelPosition(panel);
  };

  const openMenu = () => {
    menu.classList.add('is-open');
    toggle.setAttribute('aria-expanded', 'true');
    panel.hidden = false;
    window.requestAnimationFrame(() => syncMenuPanelPosition(panel));
  };

  toggle.addEventListener('click', (event) => {
    event.stopPropagation();
    if (menu.classList.contains('is-open')) {
      closeMenu();
      return;
    }
    closeAllCourseMenus(menu);
    openMenu();
  });

  panel.querySelectorAll('a').forEach((link) => link.addEventListener('click', closeMenu));

  document.addEventListener('click', (event) => {
    if (!menu.contains(event.target)) closeMenu();
  });
});

document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape') {
    modals.forEach((modal) => {
      if (modal.classList.contains('open')) closeModal(modal);
    });
    closeAllCourseMenus();
    if (document.body.classList.contains('drawer-open')) closeDrawer();
  }
});

window.addEventListener('resize', () => {
  if (window.innerWidth > 920) closeDrawer();
  coursesMenus.forEach((menu) => {
    if (!menu.classList.contains('is-open')) return;
    const panel = menu.querySelector('[data-courses-panel]');
    syncMenuPanelPosition(panel);
  });
});

window.addEventListener('scroll', () => {
  coursesMenus.forEach((menu) => {
    if (!menu.classList.contains('is-open')) return;
    const panel = menu.querySelector('[data-courses-panel]');
    syncMenuPanelPosition(panel);
  });
}, { passive: true });

const trajectorySwitchers = document.querySelectorAll('[data-trajectory-switcher]');

trajectorySwitchers.forEach((switcher) => {
  const tabs = Array.from(switcher.querySelectorAll('[data-trajectory-tab]'));
  const panels = Array.from(switcher.querySelectorAll('[data-trajectory-panel]'));
  if (!tabs.length || !panels.length) return;

  const setActiveTrajectory = (name) => {
    tabs.forEach((tab) => {
      const isActive = tab.dataset.trajectoryTab === name;
      tab.classList.toggle('is-active', isActive);
      tab.setAttribute('aria-selected', String(isActive));
      tab.setAttribute('tabindex', isActive ? '0' : '-1');
    });

    panels.forEach((panel) => {
      panel.classList.toggle('is-active', panel.dataset.trajectoryPanel === name);
    });
  };

  const initialTab = tabs.find((tab) => tab.classList.contains('is-active')) || tabs[0];

  tabs.forEach((tab) => {
    tab.setAttribute('role', 'tab');
    tab.addEventListener('click', () => setActiveTrajectory(tab.dataset.trajectoryTab));
  });

  setActiveTrajectory(initialTab.dataset.trajectoryTab);
});

const faqItems = document.querySelectorAll('.faq-item');

const syncFaqHeight = (item) => {
  const answer = item.querySelector('.faq-answer');
  if (!answer) return;
  if (item.classList.contains('open')) {
    answer.style.setProperty('--faq-height', `${answer.scrollHeight}px`);
    answer.style.maxHeight = `${answer.scrollHeight}px`;
  } else {
    answer.style.maxHeight = '0px';
  }
};

faqItems.forEach((item) => {
  const button = item.querySelector('button');
  const answer = item.querySelector('.faq-answer');
  if (!button || !answer) return;
  button.setAttribute('aria-expanded', String(item.classList.contains('open')));
  syncFaqHeight(item);
  button.addEventListener('click', () => {
    const willOpen = !item.classList.contains('open');
    item.classList.toggle('open', willOpen);
    button.setAttribute('aria-expanded', String(willOpen));
    syncFaqHeight(item);
  });
});

window.addEventListener('resize', () => {
  faqItems.forEach(syncFaqHeight);
});

const calendarPreviews = document.querySelectorAll('[data-calendar-preview]');

calendarPreviews.forEach((preview) => {
  const monthTabs = Array.from(preview.querySelectorAll('[data-calendar-tab]'));
  const monthPanels = Array.from(preview.querySelectorAll('[data-calendar-panel]'));
  const monthLabel = preview.querySelector('[data-calendar-month-label]');
  const trackLabel = preview.querySelector('[data-calendar-track]');
  const timeLabel = preview.querySelector('[data-calendar-time]');
  const formatLabel = preview.querySelector('[data-calendar-format]');

  const setActiveMonth = (name) => {
    monthTabs.forEach((tab) => {
      const isActive = tab.dataset.calendarTab === name;
      tab.classList.toggle('is-active', isActive);
      tab.setAttribute('aria-selected', String(isActive));
    });

    monthPanels.forEach((panel) => {
      const isActive = panel.dataset.calendarPanel === name;
      panel.classList.toggle('is-active', isActive);
      panel.hidden = !isActive;
    });

    const activeTab = monthTabs.find((tab) => tab.dataset.calendarTab === name);
    if (activeTab) {
      if (monthLabel) monthLabel.textContent = activeTab.dataset.calendarLabel || '';
      if (trackLabel) trackLabel.textContent = activeTab.dataset.calendarTrackValue || '';
      if (timeLabel) timeLabel.textContent = activeTab.dataset.calendarTimeValue || '';
      if (formatLabel) formatLabel.textContent = activeTab.dataset.calendarFormatValue || '';
    }
  };

  monthTabs.forEach((tab) => {
    tab.addEventListener('click', () => setActiveMonth(tab.dataset.calendarTab));
  });

  const initialMonth = monthTabs.find((tab) => tab.classList.contains('is-active')) || monthTabs[0];
  if (initialMonth) setActiveMonth(initialMonth.dataset.calendarTab);

  const agendaTabs = Array.from(preview.querySelectorAll('[data-calendar-agenda-tab]'));
  const agendaPanels = Array.from(preview.querySelectorAll('[data-calendar-agenda-panel]'));

  const setActiveAgenda = (name) => {
    agendaTabs.forEach((tab) => {
      const isActive = tab.dataset.calendarAgendaTab === name;
      tab.classList.toggle('is-active', isActive);
    });

    agendaPanels.forEach((panel) => {
      const isActive = panel.dataset.calendarAgendaPanel === name;
      panel.classList.toggle('is-active', isActive);
      panel.hidden = !isActive;
    });
  };

  agendaTabs.forEach((tab) => {
    tab.addEventListener('click', () => setActiveAgenda(tab.dataset.calendarAgendaTab));
  });

  const initialAgenda = agendaTabs.find((tab) => tab.classList.contains('is-active')) || agendaTabs[0];
  if (initialAgenda) setActiveAgenda(initialAgenda.dataset.calendarAgendaTab);
});

const showFormMessage = (form, message, isError = false) => {
  let note = form.querySelector('.form-note');
  if (!note) {
    note = document.createElement('div');
    note.className = 'form-note';
    note.setAttribute('role', 'status');
    form.appendChild(note);
  }
  note.textContent = message;
  note.classList.toggle('error', isError);
};

const phoneInputs = document.querySelectorAll('input[data-phone]');
const phoneDigits = (value) => value.replace(/\D/g, '');
const isTelegramHandle = (value) => value.startsWith('@') && value.length > 4;
const isRepeatedDigits = (digits) => /^(\d)\1+$/.test(digits);
const isDummyNumber = (digits) => {
  if (!digits) return true;
  if (isRepeatedDigits(digits)) return true;
  if (digits.length >= 11 && digits[0] === '7' && /^0+$/.test(digits.slice(1))) return true;
  if (digits.length >= 11 && digits[0] === '8' && /^0+$/.test(digits.slice(1))) return true;
  if (digits.length >= 10 && /^0+$/.test(digits)) return true;
  return false;
};

const validatePhoneInput = (input, showError = false) => {
  const value = input.value.trim();
  const mode = input.dataset.phone || 'strict';
  const wrap = input.closest('.input-wrap');
  const error = wrap ? wrap.querySelector('.input-error') : null;
  if (!value) {
    if (wrap) wrap.classList.remove('invalid');
    input.classList.remove('input-invalid');
    if (error) error.textContent = '';
    return true;
  }
  const digits = phoneDigits(value);
  const validDigits = mode === 'strict'
    ? digits.length === 11 && (digits[0] === '7' || digits[0] === '8') && !isDummyNumber(digits)
    : digits.length >= 10 && !isDummyNumber(digits);
  const isValid = mode === 'flex' ? isTelegramHandle(value) || validDigits : validDigits;
  if (!isValid && showError) {
    if (error) {
      error.textContent = mode === 'flex'
        ? 'Введите корректный номер телефона или @username.'
        : 'Введите корректный номер телефона (например, +7 999 000‑00‑00).';
    }
  }
  if (wrap) wrap.classList.toggle('invalid', !isValid);
  input.classList.toggle('input-invalid', !isValid);
  return isValid;
};

const isValidEmailValue = (value) => {
  const trimmed = value.trim();
  if (!trimmed) return false;
  const parts = trimmed.split('@');
  if (parts.length !== 2) return false;
  const local = parts[0];
  const domain = parts[1];
  if (!local || !domain) return false;
  if (domain.length < 4) return false;
  if ((domain.match(/\./g) || []).length !== 1) return false;
  if (domain.startsWith('.') || domain.endsWith('.')) return false;
  return true;
};

phoneInputs.forEach((input) => {
  input.addEventListener('input', () => validatePhoneInput(input, false));
  input.addEventListener('blur', () => validatePhoneInput(input, true));
});

const telegramInputs = document.querySelectorAll('input[data-telegram]');
const telegramPattern = /^@?[a-zA-Z0-9_]{5,32}$/;
const telegramCache = new Map();
const telegramTimers = new WeakMap();

const normalizeTelegram = (value) => value.trim().replace(/^@+/, '');

const setTelegramState = (input, state, message = '') => {
  const wrap = input.closest('.input-wrap');
  if (!wrap) return;
  const error = wrap.querySelector('.input-error');
  const status = wrap.querySelector('.input-status');
  wrap.classList.remove('invalid', 'valid', 'pending');
  if (error) error.textContent = '';
  if (status) status.textContent = '';
  if (state === 'invalid') {
    wrap.classList.add('invalid');
    if (error) error.textContent = message;
  }
  if (state === 'pending') {
    wrap.classList.add('pending');
    if (status) status.textContent = message;
  }
  if (state === 'valid') {
    wrap.classList.add('valid');
    if (status) status.textContent = message;
  }
  if (state === 'neutral' && status) {
    status.textContent = message;
  }
};

const checkTelegramUsername = async (username) => {
  const key = username.toLowerCase();
  if (telegramCache.has(key)) return telegramCache.get(key);
  try {
    const response = await fetch(`/validate/telegram?username=${encodeURIComponent(username)}`);
    const data = await response.json();
    telegramCache.set(key, data);
    return data;
  } catch (error) {
    return { ok: false, reason: 'error' };
  }
};

const scheduleTelegramCheck = (input, forceError = false) => {
  const value = input.value.trim();
  const username = normalizeTelegram(value);
  if (!value) {
    setTelegramState(input, 'idle');
    return;
  }
  if (!telegramPattern.test(value)) {
    if (forceError || value.length >= 5) {
      setTelegramState(input, 'invalid', 'Введите @username (5–32 символа, латиница/цифры/_)');
    } else {
      setTelegramState(input, 'idle');
    }
    return;
  }

  setTelegramState(input, 'pending', 'Проверяем Telegram…');
  const timer = setTimeout(async () => {
    const current = normalizeTelegram(input.value.trim());
    if (!current || current !== username) return;
    const result = await checkTelegramUsername(username);
    if (current !== normalizeTelegram(input.value.trim())) return;
    if (result.ok) {
      setTelegramState(input, 'valid', 'Аккаунт найден.');
    } else if (result.reason === 'not_configured') {
      setTelegramState(input, 'neutral', 'Проверка Telegram временно недоступна.');
    } else if (result.reason === 'telethon_login_required') {
      setTelegramState(input, 'neutral', 'Нужно авторизовать Telegram через QR‑код.');
    } else if (result.reason === 'error') {
      setTelegramState(input, 'neutral', 'Не удалось проверить Telegram. Продолжим без проверки.');
    } else {
      setTelegramState(input, 'neutral', 'Не удалось подтвердить Telegram. Проверьте написание.');
    }
  }, 450);
  telegramTimers.set(input, timer);
};

telegramInputs.forEach((input) => {
  input.addEventListener('input', () => {
    const timer = telegramTimers.get(input);
    if (timer) clearTimeout(timer);
    scheduleTelegramCheck(input, false);
  });
  input.addEventListener('blur', () => {
    const timer = telegramTimers.get(input);
    if (timer) clearTimeout(timer);
    scheduleTelegramCheck(input, true);
  });
});

const isTelegramValidInput = (input) => {
  if (!input) return false;
  const value = input.value.trim();
  if (!value) return false;
  if (!telegramPattern.test(value)) return false;
  const wrap = input.closest('.input-wrap');
  if (wrap && (wrap.classList.contains('pending') || wrap.classList.contains('invalid'))) {
    return false;
  }
  return true;
};

const applyForms = document.querySelectorAll('form[action="/apply"]');
applyForms.forEach((form) => {
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    if (form.dataset.sending === 'true') return;
    form.dataset.sending = 'true';

    const nameField = form.querySelector('input[name="name"]');
    if (nameField && !nameField.value.trim()) {
      showFormMessage(form, 'Введите имя.', true);
      form.dataset.sending = 'false';
      return;
    }

    const phoneField = form.querySelector('input[data-phone]');
    const telegramField = form.querySelector('input[data-telegram]');
    const contactRequired = form.hasAttribute('data-contact-required');
    const phoneProvided = !!(phoneField && phoneField.value.trim());
    const telegramProvided = !!(telegramField && telegramField.value.trim());

    if (contactRequired && !phoneProvided && !telegramProvided) {
      showFormMessage(form, 'Укажите телефон или Telegram.', true);
      form.dataset.sending = 'false';
      return;
    }

    if (phoneProvided && phoneField && !validatePhoneInput(phoneField, true)) {
      showFormMessage(form, 'Проверьте номер телефона.', true);
      form.dataset.sending = 'false';
      return;
    }

    if (telegramProvided && telegramField) {
      const wrap = telegramField.closest('.input-wrap');
      if (wrap && wrap.classList.contains('pending')) {
        showFormMessage(form, 'Дождитесь проверки Telegram.', true);
        form.dataset.sending = 'false';
        return;
      }
      if (!isTelegramValidInput(telegramField)) {
        showFormMessage(form, 'Проверьте Telegram username.', true);
        form.dataset.sending = 'false';
        return;
      }
    }

    const emailField = form.querySelector('input[data-email], input[type="email"]');
    if (emailField && !isValidEmailValue(emailField.value)) {
      showFormMessage(form, 'Введите корректный email (минимум 4 символа после @ и точка).', true);
      form.dataset.sending = 'false';
      return;
    }

    const submitBtn = form.querySelector('button[type="submit"]');
    if (submitBtn) submitBtn.disabled = true;

    try {
      const response = await fetch(form.action, {
        method: 'POST',
        body: new FormData(form),
        headers: { Accept: 'application/json' },
      });

      if (response.redirected) {
        window.location.href = response.url;
        return;
      }
      if (!response.ok) {
        let errorMessage = '';
        try {
          errorMessage = (await response.text()).trim();
        } catch (e) {
          errorMessage = '';
        }
        if (response.status === 429 && errorMessage) {
          throw new Error(errorMessage);
        }
        throw new Error('Не удалось отправить заявку. Попробуйте немного позднее.');
      }

      showFormMessage(form, 'Спасибо! Мы на связи.');
      form.reset();

      const modal = form.closest('.modal');
      if (modal) {
        setTimeout(() => closeModal(modal), 1800);
      }
    } catch (error) {
      const message = error && error.message
        ? error.message
        : 'Не удалось отправить заявку. Попробуйте немного позднее.';
      showFormMessage(form, message, true);
    } finally {
      form.dataset.sending = 'false';
      if (submitBtn) submitBtn.disabled = false;
    }
  });
});

const validatedForms = document.querySelectorAll('form[data-validate-phone]');
validatedForms.forEach((form) => {
  form.addEventListener('submit', (event) => {
    const phoneField = form.querySelector('input[data-phone]');
    const telegramField = form.querySelector('input[data-telegram]');
    const contactRequired = form.hasAttribute('data-contact-required');
    const phoneProvided = !!(phoneField && phoneField.value.trim());
    const telegramProvided = !!(telegramField && telegramField.value.trim());

    if (contactRequired && !phoneProvided && !telegramProvided) {
      event.preventDefault();
      showFormMessage(form, 'Укажите телефон или Telegram.', true);
      return;
    }

    if (phoneProvided && phoneField && !validatePhoneInput(phoneField, true)) {
      event.preventDefault();
      showFormMessage(form, 'Проверьте номер телефона.', true);
      return;
    }
    const emailField = form.querySelector('input[data-email], input[type="email"]');
    if (emailField && !isValidEmailValue(emailField.value)) {
      event.preventDefault();
      showFormMessage(form, 'Введите корректный email (минимум 4 символа после @ и точка).', true);
      return;
    }
    if (telegramField && telegramField.value.trim()) {
      const wrap = telegramField.closest('.input-wrap');
      if (wrap && wrap.classList.contains('pending')) {
        event.preventDefault();
        showFormMessage(form, 'Дождитесь проверки Telegram.', true);
        return;
      }
      if (!isTelegramValidInput(telegramField)) {
        event.preventDefault();
        showFormMessage(form, 'Проверьте Telegram username.', true);
      }
    }
  });
});


const parallaxItems = document.querySelectorAll('[data-parallax]');
const prefersReduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
let parallaxFrame = null;

const updateParallax = () => {
  parallaxFrame = null;
  const viewport = window.innerHeight;

  parallaxItems.forEach((el) => {
    const speed = parseFloat(el.dataset.parallax) || 0.1;
    const rect = el.getBoundingClientRect();
    const offset = (rect.top - viewport * 0.5) * speed * -0.25;
    const clamped = Math.max(Math.min(offset, 18), -18);
    el.style.setProperty('--parallax-offset', `${clamped}px`);
  });
};

const handleParallax = () => {
  if (parallaxFrame) return;
  parallaxFrame = requestAnimationFrame(updateParallax);
};

if (parallaxItems.length && !prefersReduced) {
  updateParallax();
  window.addEventListener('scroll', handleParallax, { passive: true });
  window.addEventListener('resize', handleParallax);
}

const timelines = document.querySelectorAll('.timeline');
let timelineFrame = null;

const updateTimelines = () => {
  timelines.forEach((timeline) => {
    const steps = Array.from(timeline.querySelectorAll('.step'));
    if (!steps.length) return;
    const firstTop = steps[0].offsetTop;
    const multiRow = steps.some((step) => step.offsetTop !== firstTop);
    timeline.classList.toggle('multi-row', multiRow);
  });
};

const handleTimeline = () => {
  if (timelineFrame) return;
  timelineFrame = requestAnimationFrame(() => {
    timelineFrame = null;
    updateTimelines();
  });
};

if (timelines.length) {
  updateTimelines();
  window.addEventListener('resize', handleTimeline);
}

const portfolioToggle = document.querySelector('[data-portfolio-toggle]');
const portfolioGrid = document.querySelector('[data-portfolio-grid]');

if (portfolioToggle && portfolioGrid) {
  portfolioToggle.addEventListener('click', () => {
    const isCollapsed = portfolioGrid.classList.toggle('collapsed');
    portfolioToggle.textContent = isCollapsed ? 'Показать ещё проекты' : 'Скрыть проекты';
    portfolioToggle.setAttribute('aria-expanded', String(!isCollapsed));
  });
}

const testimonialToggle = document.querySelector('[data-testimonial-toggle]');
const testimonialGrid = document.querySelector('[data-testimonial-grid]');

if (testimonialToggle && testimonialGrid) {
  testimonialToggle.addEventListener('click', () => {
    const isCollapsed = testimonialGrid.classList.toggle('collapsed');
    testimonialToggle.textContent = isCollapsed ? 'Показать все отзывы' : 'Скрыть отзывы';
    testimonialToggle.setAttribute('aria-expanded', String(!isCollapsed));
  });
}

const reveals = document.querySelectorAll('.reveal');
const observer = new IntersectionObserver(
  (entries) => {
    entries.forEach((entry) => {
      if (entry.isIntersecting) {
        entry.target.classList.add('in-view');
        observer.unobserve(entry.target);
      }
    });
  },
  { threshold: 0.15 }
);

reveals.forEach((el, index) => {
  const delay = prefersReduced ? 0 : Math.min(index * 50, 200);
  el.style.transitionDelay = `${delay}ms`;
  observer.observe(el);
});

// Telegram login widget is overlaid on the visible button; no fallback needed.

const carousels = document.querySelectorAll('[data-carousel]');

carousels.forEach((carousel) => {
  const viewport = carousel.querySelector('[data-carousel-viewport]');
  const track = carousel.querySelector('[data-carousel-track]');
  if (!viewport || !track) return;

  const slides = Array.from(track.children);
  if (!slides.length) return;

  const prevBtn = carousel.querySelector('[data-carousel-prev]');
  const nextBtn = carousel.querySelector('[data-carousel-next]');
  const dotsWrap = carousel.querySelector('[data-carousel-dots]');
  let currentIndex = 0;
  let snapPoints = [];

  const buildSnapPoints = () => {
    const maxScrollLeft = Math.max(0, viewport.scrollWidth - viewport.clientWidth);
    const points = slides
      .map((slide) => Math.min(slide.offsetLeft, maxScrollLeft))
      .sort((a, b) => a - b)
      .reduce((acc, value) => {
        if (!acc.length || Math.abs(value - acc[acc.length - 1]) > 2) {
          acc.push(value);
        }
        return acc;
      }, []);
    if (!points.length) points.push(0);
    return points;
  };

  const clampIndex = (index) => Math.max(0, Math.min(index, snapPoints.length - 1));

  const scrollToIndex = (index) => {
    const nextIndex = clampIndex(index);
    const target = snapPoints[nextIndex];
    if (target === undefined) return;
    viewport.scrollTo({
      left: target,
      behavior: prefersReduced ? 'auto' : 'smooth',
    });
  };

  const updateDots = () => {
    if (!dotsWrap) return;
    const dots = dotsWrap.querySelectorAll('button');
    dots.forEach((dot, index) => {
      dot.setAttribute('aria-current', index === currentIndex ? 'true' : 'false');
    });
  };

  const updateControls = () => {
    if (prevBtn) prevBtn.disabled = currentIndex === 0;
    if (nextBtn) nextBtn.disabled = currentIndex === snapPoints.length - 1;
    updateDots();
  };

  const buildDots = () => {
    if (!dotsWrap) return;
    dotsWrap.innerHTML = '';
    snapPoints.forEach((_, index) => {
      const dot = document.createElement('button');
      dot.type = 'button';
      dot.className = 'course-carousel__dot';
      dot.setAttribute('aria-label', `Курс ${index + 1}`);
      dot.setAttribute('aria-current', index === currentIndex ? 'true' : 'false');
      dot.addEventListener('click', () => scrollToIndex(index));
      dotsWrap.appendChild(dot);
    });
  };

  const updateIndexFromScroll = () => {
    const scrollLeft = viewport.scrollLeft;
    let closestIndex = 0;
    let closestDistance = Infinity;

    snapPoints.forEach((point, index) => {
      const distance = Math.abs(point - scrollLeft);
      if (distance < closestDistance) {
        closestDistance = distance;
        closestIndex = index;
      }
    });

    currentIndex = closestIndex;
    updateControls();
  };

  let scrollFrame = null;
  const handleScroll = () => {
    if (scrollFrame) return;
    scrollFrame = requestAnimationFrame(() => {
      scrollFrame = null;
      updateIndexFromScroll();
    });
  };

  viewport.addEventListener('scroll', handleScroll, { passive: true });
  window.addEventListener('resize', () => {
    snapPoints = buildSnapPoints();
    buildDots();
    updateIndexFromScroll();
  });
  viewport.addEventListener('keydown', (event) => {
    if (event.key === 'ArrowLeft') {
      event.preventDefault();
      scrollToIndex(currentIndex - 1);
    }
    if (event.key === 'ArrowRight') {
      event.preventDefault();
      scrollToIndex(currentIndex + 1);
    }
  });

  viewport.setAttribute('tabindex', '0');
  snapPoints = buildSnapPoints();
  buildDots();
  updateIndexFromScroll();

  if (prevBtn) prevBtn.addEventListener('click', () => scrollToIndex(currentIndex - 1));
  if (nextBtn) nextBtn.addEventListener('click', () => scrollToIndex(currentIndex + 1));
});

const initAvatarUploads = (root = document) => {
  root.querySelectorAll('[data-avatar-upload]').forEach((card) => {
    if (card.dataset.pmAvatarBound === 'true') return;
    card.dataset.pmAvatarBound = 'true';
    const input = card.querySelector('[data-avatar-input]');
    const picker = card.querySelector('[data-avatar-picker]');
    const fileName = card.querySelector('[data-avatar-file-name]');
    const preview = card.querySelector('[data-avatar-preview-image]');
    let previewUrl = '';

    picker?.addEventListener('click', () => {
      input?.click();
    });

    input?.addEventListener('change', () => {
      const file = input.files && input.files[0];
      if (fileName) fileName.textContent = file ? file.name : 'Выбрать фото';
      if (!file) return;
      if (preview) {
        if (previewUrl) URL.revokeObjectURL(previewUrl);
        previewUrl = URL.createObjectURL(file);
        preview.src = previewUrl;
        preview.hidden = false;
        preview.parentElement?.classList.add('has-image');
      }
      if (typeof card.requestSubmit === 'function') card.requestSubmit();
      else card.submit();
    });
  });
};

initAvatarUploads();

const postsCatalogs = document.querySelectorAll('[data-posts-catalog]');

postsCatalogs.forEach((catalog) => {
  const buttons = Array.from(catalog.querySelectorAll('[data-posts-category]'));
  const searchField = catalog.querySelector('[data-posts-search]');
  const cards = Array.from(catalog.querySelectorAll('[data-post-card]'));
  const countNode = catalog.querySelector('[data-posts-count]');
  const emptyNode = catalog.querySelector('[data-posts-empty]');

  if (!buttons.length || !cards.length) return;

  let activeCategory =
    buttons.find((button) => button.getAttribute('aria-pressed') === 'true')?.dataset.postsCategory || 'all';

  const applyPostsFilter = () => {
    const query = (searchField?.value || '').trim().toLowerCase();
    let visibleCount = 0;

    cards.forEach((card) => {
      const matchesCategory = activeCategory === 'all' || card.dataset.category === activeCategory;
      const haystack = card.dataset.search || '';
      const matchesQuery = !query || haystack.includes(query);
      const isVisible = matchesCategory && matchesQuery;
      card.hidden = !isVisible;
      if (isVisible) visibleCount += 1;
    });

    if (countNode) countNode.textContent = String(visibleCount);
    if (emptyNode) emptyNode.hidden = visibleCount !== 0;
  };

  buttons.forEach((button) => {
    button.addEventListener('click', () => {
      activeCategory = button.dataset.postsCategory || 'all';
      buttons.forEach((item) => {
        item.setAttribute('aria-pressed', String(item === button));
      });
      applyPostsFilter();
    });
  });

  if (searchField) {
    searchField.addEventListener('input', applyPostsFilter);
  }

  applyPostsFilter();
});

const courseCatalogs = document.querySelectorAll('[data-course-catalog]');

courseCatalogs.forEach((catalog) => {
  const buttons = Array.from(catalog.querySelectorAll('[data-course-filter]'));
  const cards = Array.from(catalog.querySelectorAll('[data-course-card]'));
  const countNode = catalog.querySelector('[data-course-filter-count]');
  const searchField = catalog.querySelector('[data-course-search]');
  const sortField = catalog.querySelector('[data-course-sort]');
  const emptyNode = catalog.querySelector('[data-course-empty]');
  const cardsGrid = cards[0]?.parentElement;
  if (!buttons.length || !cards.length) return;

  let activeGroup =
    buttons.find((button) => button.classList.contains('is-active'))?.dataset.courseFilter || 'all';
  let activeSort = sortField?.value || 'default';

  const getNumericAttr = (card, attrName, fallback) => {
    const value = Number(card.dataset[attrName]);
    return Number.isFinite(value) ? value : fallback;
  };

  const applyCourseFilter = () => {
    const query = (searchField?.value || '').trim().toLowerCase();
    let visibleCount = 0;
    const visibleCards = [];
    cards.forEach((card) => {
      const matchesGroup = activeGroup === 'all' || card.dataset.courseGroup === activeGroup;
      const haystack = card.dataset.courseName || '';
      const matchesQuery = !query || haystack.includes(query);
      const isVisible = matchesGroup && matchesQuery;
      card.hidden = !isVisible;
      if (isVisible) {
        visibleCount += 1;
        visibleCards.push(card);
      }
    });

    const comparators = {
      default: (a, b) => cards.indexOf(a) - cards.indexOf(b),
      'price-asc': (a, b) => getNumericAttr(a, 'coursePrice', 999999) - getNumericAttr(b, 'coursePrice', 999999),
      'price-desc': (a, b) => getNumericAttr(b, 'coursePrice', 0) - getNumericAttr(a, 'coursePrice', 0),
      'duration-asc': (a, b) => getNumericAttr(a, 'courseDuration', 999) - getNumericAttr(b, 'courseDuration', 999),
      'duration-desc': (a, b) => getNumericAttr(b, 'courseDuration', 0) - getNumericAttr(a, 'courseDuration', 0),
      direction: (a, b) => {
        const byDirection = getNumericAttr(a, 'courseDirectionOrder', 999) - getNumericAttr(b, 'courseDirectionOrder', 999);
        if (byDirection !== 0) return byDirection;
        return (a.dataset.courseName || '').localeCompare(b.dataset.courseName || '', 'ru');
      },
    };
    visibleCards
      .sort(comparators[activeSort] || comparators.default)
      .forEach((card) => {
        cardsGrid?.appendChild(card);
      });

    cards.forEach((card) => {
      if (card.hidden) {
        cardsGrid?.appendChild(card);
      }
    });

    if (countNode) {
      countNode.textContent = `${visibleCount} курсов`;
    }
    if (emptyNode) {
      emptyNode.hidden = visibleCount !== 0;
    }
  };

  buttons.forEach((button) => {
    button.addEventListener('click', () => {
      activeGroup = button.dataset.courseFilter || 'all';
      buttons.forEach((item) => item.classList.toggle('is-active', item === button));
      applyCourseFilter();
    });
  });

  if (searchField) {
    searchField.addEventListener('input', applyCourseFilter);
  }

  if (sortField) {
    sortField.addEventListener('change', () => {
      activeSort = sortField.value || 'default';
      applyCourseFilter();
    });
  }

  applyCourseFilter();
});

const safeSessionStorage = {
  get(key) {
    try {
      return window.sessionStorage.getItem(key);
    } catch (error) {
      return null;
    }
  },
  set(key, value) {
    try {
      window.sessionStorage.setItem(key, value);
    } catch (error) {
      // Ignore storage failures in private mode.
    }
  },
};

const numberFormatter = new Intl.NumberFormat('ru-RU');
const formatNumber = (value) => numberFormatter.format(Math.max(0, Math.round(Number(value) || 0)));
const formatCurrency = (value) => `${formatNumber(value)} ₽`;
const parseNumber = (value) => Number(String(value || '').replace(/[^\d]/g, '')) || 0;

const countdownCards = document.querySelectorAll('[data-countdown]');

countdownCards.forEach((card) => {
  let deadline = new Date(card.dataset.deadline || '');
  if (Number.isNaN(deadline.getTime())) return;
  if (deadline.getTime() <= Date.now()) {
    const now = new Date();
    deadline = new Date(now.getFullYear(), now.getMonth() + 1, 0, 23, 59, 59);
  }

  const daysNode = card.querySelector('[data-countdown-days]');
  const hoursNode = card.querySelector('[data-countdown-hours]');
  const minutesNode = card.querySelector('[data-countdown-minutes]');
  const secondsNode = card.querySelector('[data-countdown-seconds]');

  const updateCountdown = () => {
    const diff = Math.max(0, deadline.getTime() - Date.now());
    const totalSeconds = Math.floor(diff / 1000);
    const days = Math.floor(totalSeconds / 86400);
    const hours = Math.floor((totalSeconds % 86400) / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;

    if (daysNode) daysNode.textContent = String(days).padStart(2, '0');
    if (hoursNode) hoursNode.textContent = String(hours).padStart(2, '0');
    if (minutesNode) minutesNode.textContent = String(minutes).padStart(2, '0');
    if (secondsNode) secondsNode.textContent = String(seconds).padStart(2, '0');

    card.classList.toggle('is-expired', diff <= 0);
  };

  updateCountdown();
  window.setInterval(updateCountdown, 1000);
});

const stickyTelegram = document.querySelector('[data-sticky-telegram]');
const stickyTelegramClose = document.querySelector('[data-sticky-telegram-close]');
const stickyTelegramStorageKey = 'pm-sticky-telegram-hidden';

if (stickyTelegram && safeSessionStorage.get(stickyTelegramStorageKey) === '1') {
  stickyTelegram.hidden = true;
}

if (stickyTelegram && stickyTelegramClose) {
  stickyTelegramClose.addEventListener('click', () => {
    stickyTelegram.hidden = true;
    safeSessionStorage.set(stickyTelegramStorageKey, '1');
  });
}

const coursePresetButtons = document.querySelectorAll('[data-course-preset]');

coursePresetButtons.forEach((button) => {
  button.addEventListener('click', () => {
    const catalog = document.querySelector('[data-course-catalog]');
    if (!catalog) return;

    const group = button.dataset.coursePresetGroup || 'all';
    const search = button.dataset.coursePresetSearch || '';
    const filterButton = catalog.querySelector(`[data-course-filter="${group}"]`);
    const searchField = catalog.querySelector('[data-course-search]');
    const presetScope =
      button.closest('.pm-discovery-card, .pm-course-quick-filters') || button.parentElement || document;

    presetScope.querySelectorAll('[data-course-preset]').forEach((item) => {
      item.classList.toggle('is-active', item === button);
    });

    if (filterButton) filterButton.click();
    if (searchField) {
      searchField.value = search;
      searchField.dispatchEvent(new Event('input', { bubbles: true }));
    }

    const targetSection = document.getElementById('courses') || catalog;
    targetSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
  });
});

const pageMarketing = pmRuntimeConfig.pageMarketing || null;
const quizCard = document.querySelector('[data-quiz]');

if (quizCard && pageMarketing?.quiz?.results) {
  const steps = Array.from(quizCard.querySelectorAll('[data-quiz-step]'));
  const resultNode = quizCard.querySelector('[data-quiz-result]');
  const resultTitle = quizCard.querySelector('[data-quiz-result-title]');
  const resultText = quizCard.querySelector('[data-quiz-result-text]');
  const resultLink = quizCard.querySelector('[data-quiz-result-link]');
  const scoreMap = {
    python_start: 'scorePythonStart',
    fullstack: 'scoreFullstack',
    data_science: 'scoreDataScience',
    business: 'scoreBusiness',
  };
  const scores = {
    python_start: 0,
    fullstack: 0,
    data_science: 0,
    business: 0,
  };
  let currentStep = 0;

  const syncQuiz = () => {
    steps.forEach((step, index) => {
      const isActive = index === currentStep;
      step.classList.toggle('is-active', isActive);
      step.hidden = !isActive;
    });
  };

  const renderQuizResult = () => {
    const winner =
      Object.entries(scores)
        .sort((left, right) => right[1] - left[1])[0]?.[0] || 'fullstack';
    const payload = pageMarketing.quiz.results[winner] || pageMarketing.quiz.results.fullstack;
    if (!payload) return;

    if (resultTitle) resultTitle.textContent = payload.title;
    if (resultText) resultText.textContent = payload.text;
    if (resultLink) {
      resultLink.href = payload.href;
      resultLink.textContent = payload.button;
    }
    if (resultNode) resultNode.hidden = false;
    steps.forEach((step) => {
      step.classList.remove('is-active');
      step.hidden = true;
    });
  };

  quizCard.addEventListener('click', (event) => {
    const choice = event.target.closest('[data-quiz-choice]');
    if (!choice || !quizCard.contains(choice)) return;

    Object.entries(scoreMap).forEach(([track, datasetKey]) => {
      scores[track] += Number(choice.dataset[datasetKey] || 0);
    });

    if (currentStep >= steps.length - 1) {
      renderQuizResult();
      return;
    }

    currentStep += 1;
    syncQuiz();
  });

  syncQuiz();
}

const roiCalculator = document.querySelector('[data-roi-calculator]');

if (
  roiCalculator &&
  Array.isArray(pageMarketing?.roi_tracks) &&
  pageMarketing.roi_tracks.length &&
  Array.isArray(pageMarketing?.roi_scenarios) &&
  pageMarketing.roi_scenarios.length
) {
  const roundToStep = (value, step = 5000) => Math.round(value / step) * step;
  const clamp = (value, min, max) => Math.min(max, Math.max(min, value));
  const trackButtons = Array.from(roiCalculator.querySelectorAll('[data-roi-track-option]'));
  const scenarioButtons = Array.from(roiCalculator.querySelectorAll('[data-roi-scenario]'));
  const presetButtons = Array.from(roiCalculator.querySelectorAll('[data-roi-salary-preset]'));
  const salaryField = roiCalculator.querySelector('[data-roi-salary]');
  const salaryRangeField = roiCalculator.querySelector('[data-roi-salary-range]');
  const salaryValueNode = roiCalculator.querySelector('[data-roi-salary-value]');
  const salaryRangeLabel = roiCalculator.querySelector('[data-roi-salary-range-label]');
  const resultTitle = roiCalculator.querySelector('[data-roi-result-title]');
  const resultText = roiCalculator.querySelector('[data-roi-result-text]');
  const activeTrackChip = roiCalculator.querySelector('[data-roi-active-track]');
  const activeScenarioChip = roiCalculator.querySelector('[data-roi-active-scenario]');
  const metricOffer = roiCalculator.querySelector('[data-roi-metric-offer]');
  const metricSalary = roiCalculator.querySelector('[data-roi-metric-salary]');
  const metricRange = roiCalculator.querySelector('[data-roi-metric-range]');
  const metricPayback = roiCalculator.querySelector('[data-roi-metric-payback]');
  const metricShare = roiCalculator.querySelector('[data-roi-metric-share]');
  const metricYear = roiCalculator.querySelector('[data-roi-metric-year]');
  const timelineTotal = roiCalculator.querySelector('[data-roi-timeline-total]');
  const studySegment = roiCalculator.querySelector('[data-roi-study-segment]');
  const paybackSegment = roiCalculator.querySelector('[data-roi-payback-segment]');
  const studyMonths = roiCalculator.querySelector('[data-roi-study-months]');
  const paybackMonthsNode = roiCalculator.querySelector('[data-roi-payback-months]');
  const proofTitle = roiCalculator.querySelector('[data-roi-proof-title]');
  const proofText = roiCalculator.querySelector('[data-roi-proof-text]');
  const trackLink = roiCalculator.querySelector('[data-roi-track-link]');
  const tracks = pageMarketing.roi_tracks.reduce((acc, item) => {
    acc[item.key] = item;
    return acc;
  }, {});
  const scenarios = pageMarketing.roi_scenarios.reduce((acc, item) => {
    acc[item.key] = item;
    return acc;
  }, {});
  const defaultTrack = tracks.fullstack || pageMarketing.roi_tracks[0];
  const defaultScenario = scenarios.balanced || pageMarketing.roi_scenarios[0];
  let activeTrackKey = defaultTrack?.key || pageMarketing.roi_tracks[0].key;
  let activeScenarioKey = defaultScenario?.key || pageMarketing.roi_scenarios[0].key;

  const getTrackBounds = (track) => {
    const min = roundToStep(Math.max(50000, Number(track.salary_min) || track.entry_salary || 60000));
    const coreMax = Math.max(Number(track.salary_max) || track.entry_salary, track.entry_salary * 1.15);
    const max = roundToStep(Math.max(min + 30000, coreMax));
    const stretch = roundToStep(Math.max(max + 20000, max * 1.2));
    return { min, max, stretch };
  };

  const getPresetSalary = (track, preset) => {
    const bounds = getTrackBounds(track);
    if (preset === 'entry') {
      return clamp(roundToStep(track.entry_salary), bounds.min, bounds.stretch);
    }
    if (preset === 'stretch') {
      return clamp(roundToStep(Math.max(bounds.max, track.entry_salary * 1.15)), bounds.min, bounds.stretch);
    }
    return clamp(roundToStep((track.entry_salary + bounds.max) / 2), bounds.min, bounds.stretch);
  };

  const applySalaryValue = (value, track) => {
    const bounds = getTrackBounds(track);
    const normalized = clamp(roundToStep(value || track.entry_salary), bounds.min, bounds.stretch);

    if (salaryField) {
      salaryField.value = formatNumber(normalized);
    }
    if (salaryRangeField) {
      salaryRangeField.min = String(bounds.min);
      salaryRangeField.max = String(bounds.stretch);
      salaryRangeField.step = '5000';
      salaryRangeField.value = String(normalized);
    }
    if (salaryValueNode) {
      salaryValueNode.textContent = formatCurrency(normalized);
    }
    if (salaryRangeLabel) {
      salaryRangeLabel.textContent = `${formatCurrency(bounds.min)} - ${formatCurrency(bounds.max)} реалистичный старт`;
    }

    presetButtons.forEach((button) => {
      const presetSalary = getPresetSalary(track, button.dataset.roiSalaryPreset || 'middle');
      const baseLabel = button.dataset.baseLabel || button.textContent.trim();
      button.dataset.baseLabel = baseLabel;
      button.innerHTML = `<span>${baseLabel}</span><strong>${formatCurrency(presetSalary)}</strong>`;
      button.classList.toggle('is-active', presetSalary === normalized);
    });

    return normalized;
  };

  const computeRoi = (track, scenario, desiredSalary) => {
    const bounds = getTrackBounds(track);
    const salaryTarget = clamp(roundToStep(desiredSalary || track.entry_salary), bounds.min, bounds.stretch);
    const timeToOffer = Math.max(2, track.time_to_offer + Number(scenario.offer_shift || 0));
    const salaryFactor = Number(scenario.salary_factor || 1);
    const paybackShare = Math.max(0.2, Number(scenario.payback_share || 0.35));
    const firstSalary = clamp(
      roundToStep(Math.max(track.entry_salary, salaryTarget * salaryFactor)),
      bounds.min,
      bounds.stretch,
    );
    const monthlyReturn = Math.max(12000, roundToStep(firstSalary * paybackShare, 1000));
    const paybackMonths = Math.max(1, Math.ceil(track.course_cost / monthlyReturn));
    const totalMonths = timeToOffer + paybackMonths;
    const firstYearIncome = Math.max(0, Math.round(Math.max(0, 12 - timeToOffer) * firstSalary - track.course_cost));

    return {
      bounds,
      salaryTarget,
      timeToOffer,
      firstSalary,
      paybackMonths,
      totalMonths,
      firstYearIncome,
      monthlyReturn,
      paybackShare,
    };
  };

  const renderRoi = () => {
    const activeTrack = tracks[activeTrackKey] || defaultTrack;
    const activeScenario = scenarios[activeScenarioKey] || defaultScenario;
    if (!activeTrack || !activeScenario) return;

    const normalizedSalary = applySalaryValue(
      parseNumber(salaryField?.value || salaryRangeField?.value) || activeTrack.entry_salary,
      activeTrack,
    );
    const roi = computeRoi(activeTrack, activeScenario, normalizedSalary);

    roiCalculator.style.setProperty('--pm-roi-accent', activeTrack.accent || '#111111');
    roiCalculator.style.setProperty('--pm-roi-accent-soft', activeTrack.accent_soft || 'rgba(17, 17, 17, 0.08)');

    trackButtons.forEach((button) => {
      const isActive = button.dataset.roiTrackOption === activeTrack.key;
      button.classList.toggle('is-active', isActive);
      button.setAttribute('aria-pressed', isActive ? 'true' : 'false');
    });

    scenarioButtons.forEach((button) => {
      const isActive = button.dataset.roiScenario === activeScenario.key;
      button.classList.toggle('is-active', isActive);
      button.setAttribute('aria-pressed', isActive ? 'true' : 'false');
    });

    if (activeTrackChip) {
      activeTrackChip.textContent = activeTrack.label;
    }
    if (activeScenarioChip) {
      activeScenarioChip.textContent = activeScenario.label;
    }
    if (resultTitle) {
      resultTitle.textContent = `Выход в плюс примерно на ${roi.totalMonths}-м месяце от старта`;
    }
    if (resultText) {
      resultText.textContent =
        `${activeTrack.label}: первая оплачиваемая роль обычно появляется через ${roi.timeToOffer} мес. ` +
        `Ориентир на стартовый доход — ${formatCurrency(roi.firstSalary)}. Если направлять ${Math.round(roi.paybackShare * 100)}% ` +
        `этого дохода на возврат вложений, курс за ${formatCurrency(activeTrack.course_cost)} обычно закрывается ещё за ${roi.paybackMonths} мес. работы.`;
    }

    if (metricOffer) metricOffer.textContent = `${roi.timeToOffer} мес.`;
    if (metricSalary) metricSalary.textContent = formatCurrency(roi.firstSalary);
    if (metricRange) metricRange.textContent = `${activeTrack.salary_range} • ${activeTrack.salary_note}`;
    if (metricPayback) metricPayback.textContent = `${roi.paybackMonths} мес.`;
    if (metricShare) metricShare.textContent = `${Math.round(roi.paybackShare * 100)}% первого дохода идёт на ROI`;
    if (metricYear) metricYear.textContent = formatCurrency(roi.firstYearIncome);
    if (timelineTotal) timelineTotal.textContent = `${roi.totalMonths} мес. до выхода в плюс`;
    if (studyMonths) studyMonths.textContent = `${roi.timeToOffer} мес.`;
    if (paybackMonthsNode) paybackMonthsNode.textContent = `${roi.paybackMonths} мес.`;
    if (studySegment) {
      studySegment.style.flexGrow = String(roi.timeToOffer);
      studySegment.style.setProperty('--pm-roi-accent', activeTrack.accent || '#111111');
    }
    if (paybackSegment) {
      paybackSegment.style.flexGrow = String(roi.paybackMonths);
      paybackSegment.style.setProperty('--pm-roi-accent', activeTrack.accent || '#111111');
    }
    if (proofTitle) {
      proofTitle.textContent = activeTrack.slogan || activeTrack.label;
    }
    if (proofText) {
      proofText.textContent =
        `${activeTrack.summary} Кейс выпускника: ${activeTrack.alumni_result} — ${activeTrack.alumni_timeline}.`;
    }
    if (trackLink) {
      trackLink.href = activeTrack.href || '#courses';
      trackLink.textContent = `Открыть ${activeTrack.label}`;
    }
  };

  trackButtons.forEach((button) => {
    button.addEventListener('click', () => {
      activeTrackKey = button.dataset.roiTrackOption || activeTrackKey;
      renderRoi();
    });
  });

  scenarioButtons.forEach((button) => {
    button.addEventListener('click', () => {
      activeScenarioKey = button.dataset.roiScenario || activeScenarioKey;
      renderRoi();
    });
  });

  presetButtons.forEach((button) => {
    button.addEventListener('click', () => {
      const activeTrack = tracks[activeTrackKey] || defaultTrack;
      if (!activeTrack) return;
      const salary = getPresetSalary(activeTrack, button.dataset.roiSalaryPreset || 'middle');
      applySalaryValue(salary, activeTrack);
      renderRoi();
    });
  });

  if (salaryRangeField) {
    salaryRangeField.addEventListener('input', () => {
      const activeTrack = tracks[activeTrackKey] || defaultTrack;
      if (!activeTrack) return;
      applySalaryValue(parseNumber(salaryRangeField.value), activeTrack);
      renderRoi();
    });
  }

  if (salaryField) {
    salaryField.addEventListener('input', () => {
      const activeTrack = tracks[activeTrackKey] || defaultTrack;
      if (!activeTrack) return;
      applySalaryValue(parseNumber(salaryField.value), activeTrack);
      renderRoi();
    });
    salaryField.addEventListener('blur', () => {
      const activeTrack = tracks[activeTrackKey] || defaultTrack;
      if (!activeTrack) return;
      applySalaryValue(parseNumber(salaryField.value) || activeTrack.entry_salary, activeTrack);
      renderRoi();
    });
  }

  applySalaryValue(defaultTrack.entry_salary, defaultTrack);
  renderRoi();
}

const sectionJumpNavs = document.querySelectorAll('[data-section-jump-nav]');

sectionJumpNavs.forEach((nav) => {
  const links = Array.from(nav.querySelectorAll('[data-section-link]'));
  if (!links.length) return;

  const linkMap = links
    .map((link) => {
      const href = link.getAttribute('href') || '';
      if (!href.startsWith('#')) return null;
      const section = document.querySelector(href);
      if (!section) return null;
      return { link, href, section };
    })
    .filter(Boolean);

  if (!linkMap.length) return;

  const setActiveLink = (href) => {
    linkMap.forEach(({ link, href: currentHref }) => {
      link.classList.toggle('is-active', currentHref === href);
    });
  };

  linkMap.forEach(({ link, href, section }) => {
    link.addEventListener('click', (event) => {
      event.preventDefault();
      section.scrollIntoView({ behavior: 'smooth', block: 'start' });
      if (window.history?.replaceState) {
        window.history.replaceState(null, '', href);
      }
      setActiveLink(href);
    });
  });

  const initialHash = window.location.hash;
  if (initialHash && linkMap.some(({ href }) => href === initialHash)) {
    setActiveLink(initialHash);
  } else {
    setActiveLink(linkMap[0].href);
  }

  if (!('IntersectionObserver' in window)) return;

  const observer = new IntersectionObserver(
    (entries) => {
      const activeEntry = entries
        .filter((entry) => entry.isIntersecting)
        .sort((left, right) => left.boundingClientRect.top - right.boundingClientRect.top)[0];

      if (!activeEntry) return;

      const match = linkMap.find(({ section }) => section === activeEntry.target);
      if (match) {
        setActiveLink(match.href);
      }
    },
    {
      rootMargin: '-30% 0px -55% 0px',
      threshold: 0.01,
    },
  );

  linkMap.forEach(({ section }) => observer.observe(section));
});

const initTeacherHoursPresets = () => {
  const form = document.querySelector('[data-teacher-hours-form]');
  const presetWrap = document.querySelector('[data-teacher-hours-presets]');
  if (!form || !presetWrap) return;

  const dayKeys = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'];
  const weekdayKeys = ['mon', 'tue', 'wed', 'thu', 'fri'];
  const weekendKeys = ['sat', 'sun'];
  const getField = (day, type, index) => form.querySelector(`[name="${day}_${type}_${index}"]`);
  const setSlot = (day, index, start = '', end = '') => {
    const startField = getField(day, 'start', index);
    const endField = getField(day, 'end', index);
    if (startField) startField.value = start;
    if (endField) endField.value = end;
  };

  const clearDay = (day) => {
    setSlot(day, 0);
    setSlot(day, 1);
  };

  const syncLabels = () => {
    dayKeys.forEach((day) => {
      const labels = [];
      [0, 1].forEach((index) => {
        const start = getField(day, 'start', index)?.value || '';
        const end = getField(day, 'end', index)?.value || '';
        if (start && end) labels.push(`${start} - ${end}`);
      });
      const label = labels.length ? labels.join(', ') : 'Нет часов';
      const weekLabel = document.querySelector(`[data-teacher-week-label="${day}"]`);
      const weekDay = document.querySelector(`[data-teacher-week-day="${day}"]`);
      const card = getField(day, 'start', 0)?.closest('.teacher-hours-card');
      const cardLabel = card ? card.querySelector('.teacher-hours-card__head span') : null;
      if (weekLabel) weekLabel.textContent = label;
      if (cardLabel) cardLabel.textContent = labels.length ? label : 'Не задано';
      if (weekDay) weekDay.classList.toggle('is-empty', !labels.length);
    });
  };

  presetWrap.querySelectorAll('[data-teacher-hours-preset]').forEach((button) => {
    button.addEventListener('click', () => {
      const preset = button.dataset.teacherHoursPreset || 'clear';
      dayKeys.forEach(clearDay);

      if (preset === 'weekday-evening') {
        weekdayKeys.forEach((day) => setSlot(day, 0, '18:00', '21:00'));
      }
      if (preset === 'weekday-full') {
        weekdayKeys.forEach((day) => setSlot(day, 0, '10:00', '18:00'));
      }
      if (preset === 'weekend') {
        weekendKeys.forEach((day) => setSlot(day, 0, '11:00', '16:00'));
      }

      presetWrap.querySelectorAll('[data-teacher-hours-preset]').forEach((item) => {
        item.classList.toggle('is-active', item === button);
      });
      syncLabels();
    });
  });

  form.querySelectorAll('input[type="time"]').forEach((input) => {
    input.addEventListener('input', syncLabels);
  });
  syncLabels();
};

const initTeacherCardFilters = () => {
  const setupFilter = (filterbar, cardSelector, mode) => {
    const panel = filterbar.closest('.teacher-groups-panel, .teacher-students-panel') || document;
    const cards = Array.from(panel.querySelectorAll(cardSelector));
    if (!cards.length) return;
    const search = filterbar.querySelector('[data-teacher-filter-search]');
    const course = filterbar.querySelector('[data-teacher-filter-course]');
    const stateButtons = Array.from(filterbar.querySelectorAll('[data-teacher-filter-state]'));
    const count = filterbar.querySelector('[data-teacher-filter-count]');
    const empty = panel.querySelector('[data-teacher-filter-empty]');
    let activeState = 'all';

    const normalize = (value) => String(value || '').trim().toLowerCase();
    const matchesState = (card) => {
      if (activeState === 'all') return true;
      const ready = card.dataset.ready === 'yes';
      if (mode === 'students') {
        if (activeState === 'ready') return ready;
        if (activeState === 'waiting') return !ready;
        return true;
      }
      const hasCommon = card.dataset.common === 'yes';
      const hasSlot = card.dataset.slot === 'yes';
      const hasMembers = Number(card.dataset.members || 0) > 0;
      if (activeState === 'needs-slot') return hasCommon && !hasSlot;
      if (activeState === 'waiting') return hasMembers && !ready;
      if (activeState === 'no-window') return hasMembers && ready && !hasCommon;
      return true;
    };

    const apply = () => {
      const queryValue = normalize(search?.value);
      const courseValue = normalize(course?.value);
      let visible = 0;

      cards.forEach((card) => {
        const matchesQuery = !queryValue || normalize(card.dataset.search).includes(queryValue);
        const matchesCourse = !courseValue || courseValue === 'all' || normalize(card.dataset.course).includes(courseValue);
        const isVisible = matchesQuery && matchesCourse && matchesState(card);
        card.hidden = !isVisible;
        if (isVisible) visible += 1;
      });

      if (count) count.textContent = `${visible} показано`;
      if (empty) empty.hidden = visible !== 0;
    };

    if (search) search.addEventListener('input', apply);
    if (course) course.addEventListener('change', apply);
    stateButtons.forEach((button) => {
      button.addEventListener('click', () => {
        activeState = button.dataset.teacherFilterState || 'all';
        stateButtons.forEach((item) => item.classList.toggle('is-active', item === button));
        apply();
      });
    });

    apply();
  };

  document.querySelectorAll('[data-teacher-group-filter]').forEach((filterbar) => {
    setupFilter(filterbar, '[data-teacher-group-card]', 'groups');
  });
  document.querySelectorAll('[data-teacher-student-filter]').forEach((filterbar) => {
    setupFilter(filterbar, '[data-teacher-student-card]', 'students');
  });
};

initTeacherHoursPresets();
initTeacherCardFilters();
