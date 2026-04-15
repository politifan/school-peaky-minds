# GitHub OAuth Setup

## Что уже сделано в коде

- Вход через `VK` на странице логина отключен.
- Добавлен новый вход через `GitHub`.
- Callback для GitHub: `/auth/github/callback`

## Что нужно сделать в GitHub

1. Откройте [GitHub Developer Settings](https://github.com/settings/developers).
2. Перейдите в `OAuth Apps`.
3. Нажмите `New OAuth App`.
4. Заполните поля:
   - `Application name`: любое удобное, например `Peaky Minds`
   - `Homepage URL`: адрес сайта, например `https://school.peaky-minds.ru`
   - `Authorization callback URL`: `https://school.peaky-minds.ru/auth/github/callback`
5. После создания сохраните:
   - `Client ID`
   - `Client Secret`

## Что нужно прописать в конфиге

В `config/.env` должны быть значения:

```env
APP_BASE_URL=https://school.peaky-minds.ru
SESSION_SECRET=сложный_случайный_секрет

GITHUB_CLIENT_ID=ваш_github_client_id
GITHUB_CLIENT_SECRET=ваш_github_client_secret
```

Если VK-вход больше не нужен, можно удалить старые переменные:

```env
VK_CLIENT_ID=
VK_CLIENT_SECRET=
VK_SCOPE=
```

Важно:

- `APP_BASE_URL` должен совпадать с доменом, который указан в GitHub OAuth App.
- `Authorization callback URL` в GitHub должен совпадать до символа с `/auth/github/callback`.
- Если сайт работает за прокси или CDN, наружу должен приходить правильный `https`.

## Как проверить

1. Перезапустите приложение после изменения `config/.env`.
2. Откройте `/login`.
3. Убедитесь, что вместо VK видна кнопка `GitHub`.
4. Пройдите вход через GitHub.
5. После callback пользователь должен попасть на главную и авторизоваться в системе.

## Локальная проверка

Для локального запуска используйте отдельный OAuth App или временно настройте:

```env
APP_BASE_URL=http://localhost:8000
```

Тогда callback в GitHub должен быть:

```text
http://localhost:8000/auth/github/callback
```

## Что оставить от VK

Если вы используете VK не для входа, а только как контакт или канал уведомлений, можно оставить:

- `VK_MESSAGE_TOKEN`
- `CONTACT_VK`

Они не участвуют в новом GitHub-входе.
