# Component Plan: первая реализация JetBrains-style редизайна

## Статус
- Версия: `v1`
- Статус: `active`
- Основание: [ROADMAP_JETBRAINS_STYLE.md](/mnt/c/Users/mihai/OneDrive/Desktop/school-peaky-minds/docs/ROADMAP_JETBRAINS_STYLE.md)
- Основание: [DESIGN_SYSTEM_JETBRAINS_STYLE.md](/mnt/c/Users/mihai/OneDrive/Desktop/school-peaky-minds/docs/DESIGN_SYSTEM_JETBRAINS_STYLE.md)
- Основание: [WIREFRAME_HOME_JETBRAINS_STYLE.md](/mnt/c/Users/mihai/OneDrive/Desktop/school-peaky-minds/docs/WIREFRAME_HOME_JETBRAINS_STYLE.md)
- Назначение: список компонентов для первой практической реализации в коде

---

## 1. Роль документа

Этот документ отвечает на 5 вопросов:

1. Какие компоненты нужны для первой реализации.
2. В каком порядке их делать.
3. Какие из них являются базой, а какие блоками главной страницы.
4. Что можно сохранить из текущего кода, а что нужно переписать.
5. Какие шаблоны и JS-модули будут затронуты первыми.

Под “первой реализацией” в этом документе понимается:

- `P0`: системная база нового стиля;
- `P1`: новая главная страница;
- без полной переделки остальных страниц на этом шаге.

---

## 2. Границы первой реализации

## Входит в первую реализацию
- новый marketing shell;
- новые глобальные токены;
- новая типографика;
- новый header;
- новый footer;
- новые кнопки;
- новые формы;
- новые карточки;
- новые layout-wrapper компоненты;
- все ключевые блоки новой главной;
- минимально необходимые интерактивные компоненты для главной и формы заявки.

## Не входит в первую реализацию
- полная переделка кабинета;
- полная переделка договора;
- полная переделка админки;
- отдельные shell для student/admin;
- редизайн всех курсов;
- полная переработка всех success/login экранов.

---

## 3. Результат первого этапа

После завершения первой реализации должны быть готовы:

- визуально новый `base.html` для маркетинговых страниц;
- новая версия главной страницы;
- новый набор базовых UI-компонентов;
- новый фундамент CSS и частично обновленный JS;
- готовая стартовая архитектура, на которую уже можно насаживать страницы курсов.

---

## 4. Компонентные слои

Компоненты делятся на 4 уровня.

## Layer A: Foundation
- токены
- typography scale
- layout helpers
- section wrappers
- shell primitives

## Layer B: Core UI
- buttons
- pills
- links
- fields
- selects
- checkboxes
- badges
- cards

## Layer C: Marketing blocks
- header
- hero
- trajectories switcher
- project cards grid
- learning steps
- course catalog cards
- career prep banner
- metrics
- testimonials
- FAQ
- final CTA
- footer

## Layer D: Interaction
- mobile drawer
- accordion
- modal
- form validation feedback
- trajectories switcher interaction

---

## 5. Порядок реализации

Порядок обязателен. Иначе начнется сборка страниц из нестабильных частей.

### Шаг 1
Foundation tokens and layout base

### Шаг 2
Typography and page shell

### Шаг 3
Buttons, pills, fields, base cards

### Шаг 4
Header and footer

### Шаг 5
Hero and trajectories

### Шаг 6
Project cards, learning steps, course cards

### Шаг 7
Career prep banner, metrics, testimonials

### Шаг 8
FAQ and final CTA form

### Шаг 9
Modal and JS cleanup

### Шаг 10
Refinement and responsive pass

---

## 6. Список компонентов первой реализации

Ниже полный список компонентов, которые считаются обязательными.

## 6.1 Foundation components

### `shell-marketing`
Назначение:
- основная оболочка публичных страниц.

Где используется:
- `base.html`
- главная
- позже страницы курсов.

Что включает:
- фон страницы;
- общую типографику;
- правила для main;
- header spacing;
- footer spacing;
- декоративные shape-layers.

Статус для текущего проекта:
- новый компонент;
- требует переписки текущей структуры body и глобального фона.

### `container`
Назначение:
- единый горизонтальный контейнер.

Варианты:
- default
- wide
- narrow

Где используется:
- все секции главной;
- header;
- footer;
- modal content areas.

Статус:
- нужно пересобрать.

### `section-frame`
Назначение:
- единая секционная оболочка по вертикальным отступам и ритму.

Варианты:
- default
- compact
- contrast

Статус:
- новый компонент.

### `shape-stage`
Назначение:
- общий слой для декоративных абстрактных фигур.

Где используется:
- hero;
- career banner;
- final CTA;
- отдельные акцентные секции.

Статус:
- новый компонент;
- должен заменить старые `.bg-orb`.

---

## 6.2 Typography primitives

### `eyebrow`
Назначение:
- маленькая продуктовая метка перед заголовком.

Где используется:
- hero;
- intro inside banners;
- course promos.

### `display-title`
Назначение:
- H1/H2 в сильных секциях.

Варианты:
- hero
- section
- compact

### `lead-text`
Назначение:
- ведущий подзаголовок для hero и крупных секций.

### `meta-text`
Назначение:
- подписи, служебные строки, labels.

---

## 6.3 Core actions

### `c-button`
Назначение:
- основной action-компонент.

Варианты:
- primary
- secondary
- dark
- ghost

Состояния:
- default
- hover
- focus-visible
- disabled

Где используется:
- header;
- hero;
- course cards;
- FAQ/links where needed;
- final CTA;
- modal;
- footer utility actions.

Статус:
- полностью переписать.

### `c-link`
Назначение:
- текстовые вторичные действия.

Варианты:
- inline
- arrow-link
- muted-link

### `c-pill`
Назначение:
- pills, tabs, topic chips, segmented controls.

Варианты:
- neutral
- active
- dark
- outline

Состояния:
- default
- hover
- active
- disabled

Статус:
- новый ключевой компонент.

---

## 6.4 Form primitives

### `c-field`
Назначение:
- базовое поле ввода.

Варианты:
- text
- tel
- email
- textarea

Состояния:
- default
- focus
- error
- success
- disabled

Где используется:
- final CTA;
- modal;
- login/verify later;
- student shell later.

### `c-select`
Назначение:
- стилизованный select.

Состояния:
- default
- focus
- error
- disabled

### `c-checkbox`
Назначение:
- checkbox with label and helper text.

### `c-field-note`
Назначение:
- hint/error/status message under field.

### `c-form-note`
Назначение:
- общая form-level ошибка или success note.

Статус:
- часть текущей логики можно сохранить из JS;
- визуальный слой нужно переделать.

---

## 6.5 Base surfaces

### `c-card`
Назначение:
- базовая светлая карточка.

Варианты:
- default
- elevated
- soft
- contrast

Где используется:
- project cards;
- course cards;
- metrics;
- testimonials;
- audience cards;
- FAQ wrappers.

### `c-panel`
Назначение:
- более крупная поверхностная плашка для секции или promo-box.

Варианты:
- white
- soft
- gradient
- dark

### `c-badge`
Назначение:
- служебные статусы и meta labels.

Варианты:
- neutral
- success
- warning
- danger
- info

---

## 6.6 Navigation components

### `marketing-header`
Назначение:
- основной публичный header нового стиля.

Состав:
- logo block
- nav links
- account/login block
- primary CTA
- mobile toggle

Состояния:
- default
- scrolled
- mobile-open

Зависимости:
- `shell-marketing`
- `c-button`
- `c-link`
- `mobile-drawer`

Привязка:
- переписывает текущий header в [templates/base.html](/mnt/c/Users/mihai/OneDrive/Desktop/school-peaky-minds/templates/base.html)

### `mobile-drawer`
Назначение:
- мобильная навигация.

Состав:
- nav links
- CTA
- account/login

Состояния:
- closed
- open

JS:
- новый JS-модуль;
- сейчас такого компонента нет.

### `marketing-footer`
Назначение:
- большой продуктовый footer.

Состав:
- intro block;
- columns;
- contacts;
- docs;
- legal data;
- social links.

Привязка:
- заменяет текущий footer в [templates/base.html](/mnt/c/Users/mihai/OneDrive/Desktop/school-peaky-minds/templates/base.html)

---

## 6.7 Home page sections

### `home-hero`
Назначение:
- первый экран главной.

Состав:
- eyebrow;
- display title;
- lead;
- CTA row;
- trust row;
- visual shape composition.

Зависимости:
- `container`
- `section-frame`
- `c-button`
- `eyebrow`
- `display-title`
- `shape-stage`

JS:
- не требует сложного JS;
- может иметь только сдержанный reveal.

Привязка:
- переписывает текущий `.hero.home-hero`.

### `trajectory-switcher`
Назначение:
- выбор пользовательской траектории.

Состав:
- pills row;
- active content panel;
- CTA.

Варианты траекторий:
- с нуля
- смена профессии
- для детей
- для практикующих

Зависимости:
- `c-pill`
- `c-card`
- `c-button`

JS:
- новый интерактив;
- вместо старого сравнения и части trust/problem.

### `project-card`
Назначение:
- карточка реального проекта.

Состав:
- title;
- short desc;
- stack;
- outcome;
- meta.

Варианты:
- standard
- featured

JS:
- на первом шаге без сложного JS;
- старый parallax для карточек лучше не переносить.

### `projects-grid`
Назначение:
- сетка проектных карточек.

Варианты:
- 3-col
- 2-col
- 1-col

### `learning-step-card`
Назначение:
- карточка шага “как устроено обучение”.

Состав:
- step number;
- title;
- short explanation;
- output/result.

### `learning-flow`
Назначение:
- обертка для 4 step cards.

JS:
- текущая логика определения multi-row для `.timeline` больше не нужна в старом виде;
- если потребуется, можно сделать новую, но лучше строить адаптив чисто через CSS.

### `course-card`
Назначение:
- карточка направления.

Состав:
- course label;
- title;
- summary;
- stack chips;
- duration / format / price meta;
- CTA row.

Варианты:
- default
- featured

JS:
- старый carousel лучше убрать из первой новой реализации;
- desktop/tablet/mobile решать сеткой.

### `course-grid`
Назначение:
- сетка карточек курсов.

Привязка:
- заменяет старый `course-carousel`.

### `career-prep-banner`
Назначение:
- большой градиентный блок про собеседования, рынок и карьеру.

Состав:
- title;
- text;
- topic chips;
- featured content card;
- CTA.

Зависимости:
- `c-panel`
- `c-pill`
- `c-button`
- `shape-stage`

### `metric-card`
Назначение:
- карточка доверия с цифрой.

Состав:
- value;
- label;
- note.

### `metrics-grid`
Назначение:
- сетка метрик.

### `testimonial-item`
Назначение:
- один отзыв в более редакционной подаче.

Состав:
- avatar;
- name;
- direction;
- quote;
- outcome;
- optional link.

### `testimonials-grid`
Назначение:
- layout отзывов.

JS:
- старый “показать все отзывы” можно сохранить;
- старую карточную стилистику нужно выбросить.

### `audience-card`
Назначение:
- карточка под отдельную аудиторию.

Состав:
- audience title;
- scenario;
- short value;
- CTA.

### `faq-accordion`
Назначение:
- список вопросов и ответов.

Состав:
- items;
- trigger;
- answer.

JS:
- текущую логику FAQ можно сохранить как основу, но переписать селекторы и states аккуратнее.

### `final-cta-form`
Назначение:
- последний конверсионный блок страницы.

Состав:
- strong title;
- support text;
- form card;
- submit action;
- helper note.

Зависимости:
- `c-field`
- `c-select`
- `c-form-note`
- `c-button`

JS:
- текущую валидацию форм можно частично сохранить.

---

## 7. Отдельные interactive components первой реализации

## `apply-modal`
Назначение:
- модалка сценария заявки.

Что делаем на первом шаге:
- сохраняем функциональность;
- полностью меняем визуальную оболочку;
- приводим к новому стилю.

Что внутри:
- scenario chooser;
- consult panel;
- enroll panel;
- direct-contact panel.

Что из текущего можно сохранить:
- panel switching;
- open/close logic;
- form actions.

Что нужно переписать:
- визуальные классы;
- spacing;
- buttons;
- field styles;
- headings;
- overlay feel.

Привязка:
- текущее поведение в [static/assets/js/main.js](/mnt/c/Users/mihai/OneDrive/Desktop/school-peaky-minds/static/assets/js/main.js) можно сохранить как базу.

## `reveal-on-scroll`
Назначение:
- мягкое появление секций.

Что делаем:
- оставляем, но используем осторожно;
- на главной не превращаем страницу в анимационный спектакль.

## `show-more-toggle`
Назначение:
- показать больше проектов / отзывов.

Что делаем:
- оставить как utility interaction;
- упростить текст и состояния.

---

## 8. Компоненты, которые можно переиспользовать частично

## Можно сохранить логику
- modal open/close
- panel switching in modal
- FAQ toggle logic
- form validation messages
- phone validation
- telegram validation
- show more toggles

## Нужно удалить или не переносить в новую первую реализацию
- parallax project items
- старую carousel-логику для курсов
- старую timeline multi-row adjustment как визуальный паттерн
- глобальную reveal-анимацию в старом виде для каждого элемента подряд

---

## 9. Компоненты, которые нужно делать с нуля

- `shell-marketing`
- `shape-stage`
- `marketing-header`
- `mobile-drawer`
- `marketing-footer`
- `trajectory-switcher`
- `projects-grid` нового типа
- `learning-flow`
- `course-grid` вместо carousel
- `career-prep-banner`
- `metrics-grid` нового типа
- `testimonial-item` нового типа
- `audience-card`

---

## 10. Привязка к файлам проекта

## Основные файлы первой реализации
- [templates/base.html](/mnt/c/Users/mihai/OneDrive/Desktop/school-peaky-minds/templates/base.html)
- [templates/index.html](/mnt/c/Users/mihai/OneDrive/Desktop/school-peaky-minds/templates/index.html)
- [static/assets/css/styles.css](/mnt/c/Users/mihai/OneDrive/Desktop/school-peaky-minds/static/assets/css/styles.css)
- [static/assets/js/main.js](/mnt/c/Users/mihai/OneDrive/Desktop/school-peaky-minds/static/assets/js/main.js)

## Что делаем в `base.html`
- меняем shell;
- меняем header;
- меняем footer;
- обновляем modal markup;
- подготавливаем mobile drawer.

## Что делаем в `index.html`
- пересобираем блоки под новый wireframe;
- убираем старые dark-glass паттерны;
- сохраняем маршруты, якоря и формы.

## Что делаем в `styles.css`
- вводим новые токены;
- вводим новые компонентные классы;
- постепенно удаляем старые orb/glass стили.

## Что делаем в `main.js`
- сохраняем форму и modal base logic;
- добавляем drawer logic;
- добавляем trajectory switcher logic;
- убираем старый carousel/parallax, если они больше не используются;
- оставляем FAQ и show-more в упрощённом виде.

---

## 11. Definition of done по слоям

## Foundation done
Слой считается готовым, если:

- токены заведены;
- новая типографика подключена;
- новый светлый body-shell работает;
- орбы и старый глобальный фон больше не доминируют.

## Core UI done
Слой считается готовым, если:

- есть кнопки;
- есть pills;
- есть inputs/selects/checkboxes;
- есть card/panel/badge;
- есть focus/hover/disabled states.

## Header/Footer done
Слой считается готовым, если:

- новый header работает на desktop/mobile;
- footer собран в новом стиле;
- навигация и CTA сохранены.

## Home blocks done
Слой считается готовым, если:

- hero соответствует новому wireframe;
- траектории работают;
- курсы не зависят от старого carousel;
- проекты, отзывы, FAQ и final CTA читаются как единая система.

## Interaction done
Слой считается готовым, если:

- modal работает;
- FAQ работает;
- формы валидируются;
- show-more работает;
- нет JS-мертвого кода от старой структуры.

---

## 12. Очередность правок в коде

Это рабочая последовательность для следующего этапа.

### Этап A
- токены;
- global reset;
- typography;
- shell-marketing.

### Этап B
- buttons;
- pills;
- fields;
- card/panel/badge.

### Этап C
- header;
- mobile drawer;
- footer.

### Этап D
- hero;
- trajectory switcher;
- projects;
- learning flow;
- course grid.

### Этап E
- career prep banner;
- metrics;
- testimonials;
- audience;
- FAQ;
- final CTA.

### Этап F
- modal restyle;
- JS cleanup;
- responsive cleanup.

---

## 13. Риски первой реализации

- если начать с секций, а не с foundation, получим повтор старой проблемы с хаосом классов;
- если оставить старый carousel, он будет конфликтовать с новым ритмом;
- если сохранить старые dark-glass карточки локально “на потом”, интерфейс визуально развалится;
- если не очистить `main.js` от мертвых паттернов, поддержка станет дороже.

---

## 14. Критерий завершения третьего пункта

Третий пункт считается выполненным, если:

- есть отдельный компонентный план в файле;
- список компонентов разбит на слои;
- определён порядок реализации;
- есть связка с текущими шаблонами и JS;
- понятно, что переиспользуем, что выбрасываем, что создаём с нуля.

Этот критерий в рамках данного документа выполнен.

---

## 15. Следующий рабочий шаг

После этого документа можно переходить к первой реальной кодовой фазе:

1. переписать foundation и shell в [static/assets/css/styles.css](/mnt/c/Users/mihai/OneDrive/Desktop/school-peaky-minds/static/assets/css/styles.css)
2. пересобрать [templates/base.html](/mnt/c/Users/mihai/OneDrive/Desktop/school-peaky-minds/templates/base.html)
3. затем переписать [templates/index.html](/mnt/c/Users/mihai/OneDrive/Desktop/school-peaky-minds/templates/index.html)

---

## 16. Решение по документу

Этот файл считать главным компонентным планом первой реализации JetBrains-style редизайна.
