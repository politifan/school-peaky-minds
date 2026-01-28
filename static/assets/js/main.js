const modalTriggers = document.querySelectorAll('[data-open-modal]');
const modals = document.querySelectorAll('.modal');
const closeButtons = document.querySelectorAll('[data-close-modal]');

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
  document.body.style.overflow = 'hidden';
};

const closeModal = (modal) => {
  modal.classList.remove('open');
  document.body.style.overflow = '';
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

document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape') {
    modals.forEach((modal) => {
      if (modal.classList.contains('open')) closeModal(modal);
    });
  }
});

const faqButtons = document.querySelectorAll('.faq-item button');
faqButtons.forEach((btn) => {
  btn.addEventListener('click', () => {
    const item = btn.closest('.faq-item');
    if (!item) return;
    item.classList.toggle('open');
  });
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
  const digits = phoneDigits(value);
  const validDigits = mode === 'strict'
    ? digits.length === 11 && (digits[0] === '7' || digits[0] === '8') && !isDummyNumber(digits)
    : digits.length >= 10 && !isDummyNumber(digits);
  const isValid = mode === 'flex' ? isTelegramHandle(value) || validDigits : validDigits;
  const wrap = input.closest('.input-wrap');
  const error = wrap ? wrap.querySelector('.input-error') : null;
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

const applyForms = document.querySelectorAll('form[action="/apply"]');
applyForms.forEach((form) => {
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    if (form.dataset.sending === 'true') return;
    form.dataset.sending = 'true';

    const phoneField = form.querySelector('input[data-phone]');
    if (phoneField && !validatePhoneInput(phoneField, true)) {
      showFormMessage(form, 'Проверьте номер телефона или Telegram.', true);
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

      if (!response.ok) throw new Error('Bad response');

      showFormMessage(form, 'Спасибо! Мы на связи.');
      form.reset();

      const modal = form.closest('.modal');
      if (modal) {
        setTimeout(() => closeModal(modal), 1800);
      }
    } catch (error) {
      showFormMessage(form, 'Не удалось отправить. Попробуйте ещё раз.', true);
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
    if (phoneField && !validatePhoneInput(phoneField, true)) {
      event.preventDefault();
      showFormMessage(form, 'Проверьте номер телефона.', true);
      return;
    }
    const telegramField = form.querySelector('input[data-telegram]');
    if (telegramField && telegramField.value.trim()) {
      const wrap = telegramField.closest('.input-wrap');
      if (wrap && wrap.classList.contains('pending')) {
        event.preventDefault();
        showFormMessage(form, 'Дождитесь проверки Telegram.', true);
        return;
      }
      if (wrap && wrap.classList.contains('invalid')) {
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

const telegramLoginWrap = document.querySelector('.auth-telegram');
if (telegramLoginWrap) {
  const checkTelegramWidget = () => {
    const iframe = telegramLoginWrap.querySelector('iframe');
    if (!iframe) return false;
    const rect = iframe.getBoundingClientRect();
    const visible = rect.width > 30 && rect.height > 20;
    return visible;
  };

  const activateFallback = () => {
    telegramLoginWrap.classList.add('is-fallback');
  };

  const tryDetectWidget = () => {
    if (checkTelegramWidget()) return;
    activateFallback();
  };

  setTimeout(tryDetectWidget, 1200);
  setTimeout(tryDetectWidget, 2500);
}

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
