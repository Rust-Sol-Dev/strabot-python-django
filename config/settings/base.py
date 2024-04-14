"""
Base settings to build other settings files upon.
"""
from __future__ import annotations

from stratbot.base.types.environment import Environment, EnvironmentType

from .root import *  # noqa  # isort: skip
from .root import APPS_DIR, ROOT_DIR, env  # isort: skip

# Environment
# ------------------------------------------------------------------------------
_environment_string: EnvironmentType = env("ENVIRONMENT")
assert _environment_string in ([*Environment]), "Current Pre-Condition"
ENVIRONMENT = Environment(_environment_string)

# GENERAL
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#debug
DEBUG = env.bool("DJANGO_DEBUG", False)
# Local time zone. Choices are
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name
# though not all of them may be available with every OS.
# In Windows, this must be set to your system time zone.
TIME_ZONE = "UTC"
# https://docs.djangoproject.com/en/dev/ref/settings/#language-code
LANGUAGE_CODE = "en-us"
# https://docs.djangoproject.com/en/dev/ref/settings/#use-i18n
USE_I18N = True
# https://docs.djangoproject.com/en/dev/ref/settings/#use-l10n
USE_L10N = True
# https://docs.djangoproject.com/en/dev/ref/settings/#use-tz
USE_TZ = True
# https://docs.djangoproject.com/en/dev/ref/settings/#locale-paths
LOCALE_PATHS = [str(ROOT_DIR / "locale")]

# URL/Site Info
# (NOTE: Only `SITE_ID` below is a built-in Django settings. The others are present and
# will be picked up by other places in the settings and the code.)
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#site-id
SITE_ID = 1

# Something like: https://some-website.com, etc.
# ! Important NOTE: This is the base _backend_ URL. If there's a separate frontend web
# application hosted at a different URL I'd recommend adding a seting like `WEB_APP_URL`
# and potentially also updating the `sites` migrations to have a migration for adding
# that web app URL.
BASE_BACKEND_URL = env("BASE_BACKEND_URL").removesuffix("/")
# Something like: some-website.com, etc.
DEFAULT_SITE_DOMAIN = env("DEFAULT_SITE_DOMAIN").removesuffix("/")
DEFAULT_SITE_NAME = env("DEFAULT_SITE_NAME")
# Can remove these assertions if, for whatever reason, one or more of them are actually
# incorrect assertion to make down the line, etc. They're here for safety right now.
assert BASE_BACKEND_URL.startswith("http"), "Current Pre-condition"
if ENVIRONMENT.is_prod:
    assert BASE_BACKEND_URL.startswith("https"), "Current Pre-condition"
assert DEFAULT_SITE_DOMAIN in BASE_BACKEND_URL, "Current Pre-condition"

# DATABASES
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#databases
POSTGRES_HOST = env("POSTGRES_HOST")
POSTGRES_PORT = env("POSTGRES_PORT")
POSTGRES_DB = env("POSTGRES_DB")
POSTGRES_USER = env("POSTGRES_USER")
POSTGRES_PASSWORD = env("POSTGRES_PASSWORD")

DATABASES = {
    'default': {
        'ENGINE': 'timescale.db.backends.postgresql',
        'HOST': POSTGRES_HOST,
        'PORT': POSTGRES_PORT,
        'NAME': POSTGRES_DB,
        'USER': POSTGRES_USER,
        'PASSWORD': POSTGRES_PASSWORD,
        'ATOMIC_REQUESTS': True,
    }
}
# https://docs.djangoproject.com/en/stable/ref/settings/#std:setting-DEFAULT_AUTO_FIELD
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# CACHES
# ------------------------------------------------------------------------------
CACHES = {
    "default": {
        # "BACKEND": "django_redis.cache.RedisCache",
        'BACKEND': 'django_prometheus.cache.backends.redis.RedisCache',
        "LOCATION": env("REDIS_CACHE_URL"),
        "OPTIONS": {
            "CLIENT_CLASS": "django_redis.client.DefaultClient",
            # Mimicing memcache behavior.
            # https://github.com/jazzband/django-redis#memcached-exceptions-behavior
            "IGNORE_EXCEPTIONS": True,
        },
    },
    "markets": {
        # "BACKEND": "django_redis.cache.RedisCache",
        'BACKEND': 'django_prometheus.cache.backends.redis.RedisCache',
        "LOCATION": env("REDIS_MARKET_DATA_URL"),
        "OPTIONS": {
            "CLIENT_CLASS": "django_redis.client.DefaultClient",
            # Mimicing memcache behavior.
            # https://github.com/jazzband/django-redis#memcached-exceptions-behavior
            "IGNORE_EXCEPTIONS": True,
        },
    }
}

REDIS_HOST = env("REDIS_HOST")

# Channels
# ------------------------------------------------------------------------------
# https://channels.readthedocs.io/en/stable/topics/channel_layers.html#configuration
# Configure channels layer with Redis
CHANNEL_LAYERS = {
    'default': {
        'BACKEND': 'channels_redis.core.RedisChannelLayer',
        'CONFIG': {
            "hosts": [('127.0.0.1', 6379)],
            "capacity": 1500,
            "expiry": 10,
        },
    },
}

# URLS
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#root-urlconf
ROOT_URLCONF = "config.urls"
# https://docs.djangoproject.com/en/dev/ref/settings/#wsgi-application
WSGI_APPLICATION = "config.wsgi.application"
ASGI_APPLICATION = "config.asgi.application"

# APPS
# ------------------------------------------------------------------------------
DJANGO_APPS = [
    'daphne',
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.sites",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django.contrib.humanize", # Handy template tags
    "django.contrib.admin",
    "django.forms",
]
THIRD_PARTY_APPS = [
    "allauth",
    "allauth.account",
    "allauth.socialaccount",
    'allauth.socialaccount.providers.google',
    'allauth.socialaccount.providers.apple',
    # 'allauth.socialaccount.providers.facebook',
    # 'allauth.socialaccount.providers.discord',
    'channels',
    "compressor",
    "corsheaders",
    "crispy_forms",
    "crispy_tailwind",
    "django_celery_beat",
    "django_htmx",
    "django_prometheus",
    "drf_spectacular",
    "rest_framework",
    "rest_framework.authtoken",
]

LOCAL_APPS = [
    "stratbot.base",
    "stratbot.users",
    "stratbot.scanner",
    "stratbot.alerts",
    'stratbot.events',
]
# https://docs.djangoproject.com/en/dev/ref/settings/#installed-apps
INSTALLED_APPS = DJANGO_APPS + THIRD_PARTY_APPS + LOCAL_APPS

# MIGRATIONS
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#migration-modules
MIGRATION_MODULES = {"sites": "stratbot.contrib.sites.migrations"}

# AUTHENTICATION
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#authentication-backends
AUTHENTICATION_BACKENDS = [
    "django.contrib.auth.backends.ModelBackend",
    "allauth.account.auth_backends.AuthenticationBackend",
]
# https://docs.djangoproject.com/en/dev/ref/settings/#auth-user-model
AUTH_USER_MODEL = "users.User"
# https://docs.djangoproject.com/en/dev/ref/settings/#login-redirect-url
LOGIN_REDIRECT_URL = "scanner:default-redirect"
# https://docs.djangoproject.com/en/dev/ref/settings/#login-url
LOGIN_URL = "account_login"

# PASSWORDS
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#password-hashers
PASSWORD_HASHERS = [
    # https://docs.djangoproject.com/en/dev/topics/auth/passwords/#using-argon2-with-django
    "django.contrib.auth.hashers.Argon2PasswordHasher",
    "django.contrib.auth.hashers.PBKDF2PasswordHasher",
    "django.contrib.auth.hashers.PBKDF2SHA1PasswordHasher",
    "django.contrib.auth.hashers.BCryptSHA256PasswordHasher",
]
# https://docs.djangoproject.com/en/dev/ref/settings/#auth-password-validators
AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
        'OPTIONS': {
            'min_length': 12,  # Change to the desired minimum length
        },
     },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]

# django-allauth
# ------------------------------------------------------------------------------
# TODO: change default to True after launching
ACCOUNT_ALLOW_REGISTRATION = env.bool("DJANGO_ACCOUNT_ALLOW_REGISTRATION", True)
# https://django-allauth.readthedocs.io/en/latest/configuration.html
ACCOUNT_AUTHENTICATION_METHOD = "email"
# https://django-allauth.readthedocs.io/en/latest/configuration.html
ACCOUNT_EMAIL_REQUIRED = True
# https://django-allauth.readthedocs.io/en/latest/configuration.html
ACCOUNT_EMAIL_VERIFICATION = "mandatory"
# https://django-allauth.readthedocs.io/en/latest/configuration.html
# https://django-allauth.readthedocs.io/en/latest/views.html#logout-account-logout
ACCOUNT_LOGOUT_ON_GET = False
ACCOUNT_ADAPTER = "stratbot.users.adapters.AccountAdapter"
# https://django-allauth.readthedocs.io/en/latest/forms.html
ACCOUNT_FORMS = {"signup": "stratbot.users.forms.UserSignupForm"}
# https://django-allauth.readthedocs.io/en/latest/configuration.html
ACCOUNT_USER_MODEL_USERNAME_FIELD = None
# https://django-allauth.readthedocs.io/en/latest/configuration.html
ACCOUNT_USERNAME_REQUIRED = False
# https://django-allauth.readthedocs.io/en/latest/configuration.html
SOCIALACCOUNT_ADAPTER = "stratbot.users.adapters.SocialAccountAdapter"
# https://django-allauth.readthedocs.io/en/latest/forms.html
SOCIALACCOUNT_FORMS = {"signup": "stratbot.users.forms.UserSocialSignupForm"}

SOCIALACCOUNT_PROVIDERS = {
    # TODO: define here or in database under 'social providers'
    # 'google': {
    #     'APP': {
    #         'client_id': '897943097528-tul9bueieg17glaeqmmjeqhu1ji930od.apps.googleusercontent.com',
    #         'secret': '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',
    #         'key': ''
    #     },
    #     'SCOPE': ['email', 'profile'],
    #     'AUTH_PARAMS': {'access_type': 'online'},
    # },
    "apple": {
        "APP": {
            "client_id": 'com.stratalerts.allauth',
            "secret": 'GZ3T5RY9U4',
            "key": "YR95M6LC9T",
            "certificate_key": """-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgYEjeZiyrFWigOr/c
lPK9D4OtEJdhHqnX7IZsLXgmnSShRANCAASoPw9SrGatJASWuIPkyUmVY7fVGAq7
SLPRemgKZD8cX5bpzQ3+t8yxnjiOSxtdh5MGnLm0AIx+/vxqPpA8N5tX
-----END PRIVATE KEY-----""",
        },
    },
}

# MIDDLEWARE
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#middleware
MIDDLEWARE = [
    'django_prometheus.middleware.PrometheusBeforeMiddleware',
    "django.middleware.security.SecurityMiddleware",
    "corsheaders.middleware.CorsMiddleware",
    "allauth.account.middleware.AccountMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.locale.LocaleMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.common.BrokenLinkEmailsMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "django_htmx.middleware.HtmxMiddleware",
    "django_prometheus.middleware.PrometheusAfterMiddleware",
]

# STATIC
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#static-root
STATIC_ROOT = str(ROOT_DIR / "staticfiles")
# https://docs.djangoproject.com/en/dev/ref/settings/#static-url
STATIC_URL = "/static/"
# https://docs.djangoproject.com/en/dev/ref/contrib/staticfiles/#std:setting-STATICFILES_DIRS
STATICFILES_DIRS = [str(APPS_DIR / "static")]
# https://docs.djangoproject.com/en/dev/ref/contrib/staticfiles/#staticfiles-finders
STATICFILES_FINDERS = [
    "django.contrib.staticfiles.finders.FileSystemFinder",
    "django.contrib.staticfiles.finders.AppDirectoriesFinder",
    'compressor.finders.CompressorFinder',
]

# https://django-compressor.readthedocs.io/en/stable/settings.html#django.conf.settings.COMPRESS_ENABLED
# COMPRESS_ENABLED = True

# MEDIA
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#media-root
MEDIA_ROOT = str(APPS_DIR / "media")
# https://docs.djangoproject.com/en/dev/ref/settings/#media-url
MEDIA_URL = "/media/"

# TEMPLATES
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#templates
TEMPLATES = [
    {
        # https://docs.djangoproject.com/en/dev/ref/settings/#std:setting-TEMPLATES-BACKEND
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        # https://docs.djangoproject.com/en/dev/ref/settings/#dirs
        "DIRS": [str(APPS_DIR / "templates")],
        # https://docs.djangoproject.com/en/dev/ref/settings/#app-dirs
        "APP_DIRS": True,
        "OPTIONS": {
            # https://docs.djangoproject.com/en/dev/ref/settings/#template-context-processors
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.template.context_processors.i18n",
                "django.template.context_processors.media",
                "django.template.context_processors.static",
                "django.template.context_processors.tz",
                "django.contrib.messages.context_processors.messages",
                "stratbot.users.context_processors.allauth_settings",
                "stratbot.users.context_processors.user_info",
                "stratbot.alerts.context_processors.pusher",
            ],
        },
    }
]

# https://docs.djangoproject.com/en/dev/ref/settings/#form-renderer
FORM_RENDERER = "django.forms.renderers.TemplatesSetting"

# crispy_tailwind settings: https://github.com/django-crispy-forms/crispy-tailwind/
CRISPY_TEMPLATE_PACK = "tailwind"
CRISPY_ALLOWED_TEMPLATE_PACKS = "tailwind"

# FIXTURES
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#fixture-dirs
FIXTURE_DIRS = (str(APPS_DIR / "fixtures"),)

# SECURITY
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#session-cookie-httponly
SESSION_COOKIE_HTTPONLY = True
# https://docs.djangoproject.com/en/dev/ref/settings/#csrf-cookie-httponly
CSRF_COOKIE_HTTPONLY = True
# https://docs.djangoproject.com/en/dev/ref/settings/#secure-browser-xss-filter
SECURE_BROWSER_XSS_FILTER = True
# https://docs.djangoproject.com/en/dev/ref/settings/#x-frame-options
X_FRAME_OPTIONS = "DENY"

# EMAIL
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#email-backend
EMAIL_BACKEND = env(
    "DJANGO_EMAIL_BACKEND",
    default="django.core.mail.backends.smtp.EmailBackend",
)
# https://docs.djangoproject.com/en/dev/ref/settings/#email-timeout
EMAIL_TIMEOUT = 5

# ADMIN
# ------------------------------------------------------------------------------
# Django Admin URL.
ADMIN_URL = env("DJANGO_ADMIN_URL", default="admin/")
# https://docs.djangoproject.com/en/dev/ref/settings/#admins
ADMINS = [("Tom K", "admin@stratalerts.com")]
# https://docs.djangoproject.com/en/dev/ref/settings/#managers
MANAGERS = ADMINS

# LOGGING
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#logging
# See https://docs.djangoproject.com/en/dev/topics/logging for
# more details on how to customize your logging configuration.
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "%(levelname)s %(asctime)s %(module)s "
            "%(process)d %(thread)d %(message)s"
        }
    },
    "handlers": {
        "console": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        }
    },
    "root": {"level": "INFO", "handlers": ["console"]},
}

# Celery
# ------------------------------------------------------------------------------
if USE_TZ:
    # https://docs.celeryq.dev/en/stable/userguide/configuration.html#std:setting-timezone
    CELERY_TIMEZONE = TIME_ZONE
# https://docs.celeryq.dev/en/stable/userguide/configuration.html#std:setting-broker_url
CELERY_BROKER_URL = env("CELERY_BROKER_URL")
# https://docs.celeryq.dev/en/stable/userguide/configuration.html#std:setting-result_backend
# (If using Django Celery Results (`CELERY_RESULT_BACKEND == "django-db"`), see
# https://docs.celeryq.dev/en/latest/django/first-steps-with-django.html#django-celery-results-using-the-django-orm-cache-as-a-result-backend)  # noqa E501
CELERY_RESULT_BACKEND = env("CELERY_RESULT_BACKEND")
# https://docs.celeryq.dev/en/stable/userguide/configuration.html#result-extended
CELERY_RESULT_EXTENDED = env.bool("CELERY_RESULT_EXTENDED", default=False)
# https://docs.celeryq.dev/en/stable/userguide/configuration.html#result-expires
CELERY_RESULT_EXPIRES = env.int("CELERY_RESULT_EXPIRES", default=60 * 60 * 24)  # 1 Day
# https://docs.celeryq.dev/en/stable/userguide/configuration.html#std:setting-accept_content
CELERY_ACCEPT_CONTENT = ["json"]
# https://docs.celeryq.dev/en/stable/userguide/configuration.html#std:setting-task_serializer
CELERY_TASK_SERIALIZER = "json"
# https://docs.celeryq.dev/en/stable/userguide/configuration.html#std:setting-result_serializer
CELERY_RESULT_SERIALIZER = "json"
# https://docs.celeryq.dev/en/stable/userguide/configuration.html#task-time-limit
# TODO: set to whatever value is adequate in your circumstances
CELERY_TASK_TIME_LIMIT = 2 * 60  # 2 Minutes
# https://docs.celeryq.dev/en/stable/userguide/configuration.html#task-soft-time-limit
# TODO: set to whatever value is adequate in your circumstances
CELERY_TASK_SOFT_TIME_LIMIT = 1 * 60 + 30  # 1 Minute and 30 Seconds
# https://docs.celeryq.dev/en/stable/userguide/configuration.html#beat-scheduler
CELERY_BEAT_SCHEDULER = "django_celery_beat.schedulers:DatabaseScheduler"
# https://docs.celeryq.dev/en/stable/userguide/configuration.html#worker-send-task-events
CELERY_WORKER_SEND_TASK_EVENTS = env.bool(
    "CELERY_WORKER_SEND_TASK_EVENTS", default=False
)
# https://docs.celeryq.dev/en/stable/userguide/configuration.html#task-send-sent-event
CELERY_TASK_SEND_SENT_EVENT = env.bool("CELERY_TASK_SEND_SENT_EVENT", default=False)

# Django Celery Results
# (If using. See comment above `CELERY_RESULT_BACKEND` above and https://docs.celeryq.dev/en/latest/django/first-steps-with-django.html#django-celery-results-using-the-django-orm-cache-as-a-result-backend)  # noqa E501
if CELERY_RESULT_BACKEND == "django-db":
    INSTALLED_APPS += ["django_celery_results"]

# django-rest-framework
# -------------------------------------------------------------------------------
# django-rest-framework - https://www.django-rest-framework.org/api-guide/settings/
REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": (
        "rest_framework.authentication.SessionAuthentication",
        "rest_framework.authentication.TokenAuthentication",
    ),
    "DEFAULT_PERMISSION_CLASSES": ("rest_framework.permissions.IsAuthenticated",),
    "DEFAULT_SCHEMA_CLASS": "drf_spectacular.openapi.AutoSchema",
}

# django-cors-headers - https://github.com/adamchainz/django-cors-headers#setup
CORS_URLS_REGEX = r"^/api/.*$"

# By Default swagger ui is available only to admin user(s). You can change permission classes to change that
# See more configuration options at https://drf-spectacular.readthedocs.io/en/latest/settings.html#settings
SPECTACULAR_SETTINGS = {
    "TITLE": "STRATBOT API",
    "DESCRIPTION": "Documentation of API endpoints of STRATBOT",
    "VERSION": "0.1.0",
    "SERVE_PERMISSIONS": ["rest_framework.permissions.IsAdminUser"],
    "SERVERS": [
        {"url": BASE_BACKEND_URL, "description": "Local Development server"},
        {
            "url": "https://app.stratalerts.com",
            "description": "Production server",
        },
    ],
}

# Your stuff...
# ------------------------------------------------------------------------------

# Django Extensions
# ------------------------------------------------------------------------------

# https://django-extensions.readthedocs.io/en/latest/shell_plus.html#additional-imports
SHELL_PLUS_IMPORTS = [
    "from datetime import datetime, timedelta",
    "from stratbot.alerts.integrations.pusher.bridges import channels_bridge as pusher_channels_bridge",
    "from stratbot.alerts.integrations.pusher.clients import client as pusher_client",
    "from stratbot.alerts.ops import user_setup_alerts as user_setup_alert_ops",
    "from stratbot.scanner.integrations.binance.bridges import binance_bridge",
    "from stratbot.scanner.integrations.polygon.clients import client as polygon_client",
    "from stratbot.scanner.integrations.polygon.bridges import polygon_bridge",
    "from stratbot.scanner.models.timeframes import Timeframe",
    "from stratbot.scanner.ops.live_loop.crypto import CryptoLoop",
    "from stratbot.scanner.ops.live_loop.stocks import StocksLoop",
]

# ArcticDB
# https://arctic.readthedocs.io/en/latest/configuration.html
# ------------------------------------------------------------------------------
ARCTIC_DB_URI = env("ARCTIC_DB_URI", default='mem://')
PARQUET_DIR = ROOT_DIR / "data"

# Redpanda
# ------------------------------------------------------------------------------
REDPANDA_BROKERS = env.list("REDPANDA_BROKERS")
REDPANDA_BROKERS_STR = ','.join(REDPANDA_BROKERS)
REDPANDA_USERNAME = env("REDPANDA_USERNAME")
REDPANDA_PASSWORD = env("REDPANDA_PASSWORD")
REDPANDA_SECURITY_PROTOCOL = env("REDPANDA_SECURITY_PROTOCOL", default="SASL_PLAINTEXT")
REDPANDA_SASL_MECHANISM = env("REDPANDA_SASL_MECHANISM", default="SCRAM-SHA-256")

# Pusher
# ------------------------------------------------------------------------------
PUSHER_APP_ID = env("PUSHER_APP_ID")
PUSHER_KEY = env("PUSHER_KEY")
PUSHER_SECRET = env("PUSHER_SECRET")
PUSHER_CLUSTER = env("PUSHER_CLUSTER")

# Polygon
# ------------------------------------------------------------------------------
POLYGON_API_KEY = env("POLYGON_API_KEY")

# Twelve Data
# ------------------------------------------------------------------------------
TWELVEDATA_API_KEY = env("TWELVEDATA_API_KEY")

# TDA
# ------------------------------------------------------------------------------
TDA_CLIENT_ID = env("TDA_CLIENT_ID")
TDA_REDIRECT_URI = env("TDA_REDIRECT_URI")
TDA_CREDENTIALS_PATH = env(
    "TDA_CREDENTIALS_PATH",
    default=str(ROOT_DIR / "config" / "integrations" / "tda" / "tda_credentials.json"),
)
TDA_EXISTING_CREDENTIALS_JSON_STR = env(
    "TDA_EXISTING_CREDENTIALS_JSON_STR", default="{}"
)

# Alpaca
# ------------------------------------------------------------------------------
ALPACA_API_KEY = env("ALPACA_API_KEY")
ALPACA_API_SECRET = env("ALPACA_API_SECRET")

# CCXT
# ------------------------------------------------------------------------------
# NOTE: No credentials currently present but if we add integrations/functionality
# connected to any of the exchanges supported by CCXT we could put their credentials
# here.
