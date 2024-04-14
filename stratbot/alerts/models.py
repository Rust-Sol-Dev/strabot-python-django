from __future__ import annotations
from typing import ClassVar
from datetime import datetime, timezone
import logging

from discord_webhook import DiscordWebhook, DiscordEmbed
from django.db import models
from model_utils.fields import AutoCreatedField, AutoLastModifiedField

from stratbot.scanner.models.symbols import SymbolRec, Setup
from stratbot.users.models import User

log = logging.getLogger(__name__)


class AlertType(models.TextChoices):
    IN_FORCE = "in_force", "In Force"
    MAGNITUDE = "magnitude", "Magnitude"


class UserSetupAlert(models.Model):
    # https://pusher.com/docs/channels/server_api/http-api/#examples-publish-an-event-on-a-single-channel
    # has an example of including the `event_name`. At the time of writing, this can
    # technically be up to `200` characters long.
    realtime_event_name: ClassVar[str] = "alerts--user_setup_alert"

    user = models.ForeignKey(
        User, on_delete=models.CASCADE, db_index=False, verbose_name="User"
    )
    symbol_rec = models.ForeignKey(
        SymbolRec, on_delete=models.CASCADE, verbose_name="Symbol Record"
    )
    # !Check: Do you think this is how we'd want to model this? We'd have to
    # historically persist expired `Setup`s instead of deleting (not that we were
    # planning on deleting but just noting that).
    setup = models.ForeignKey(Setup, on_delete=models.CASCADE, verbose_name="Setup")
    alert_type = models.CharField(
        "Alert Type", max_length=15, choices=AlertType.choices
    )

    # !Check: My thought here is that we provide some arbitrary JSON data such that the
    # frontend can render exactly what is needed and nothing more. I.E. the frontend
    # shouldn't have to do any conversions or logic, just display based on the key/value
    # pairs (or nested objects, etc.) present in `data`.
    data = models.JSONField(verbose_name="Data")

    # NOTE: At the time of writing, realtime notifications are delivered through Pusher
    # Channels (https://pusher.com/docs/channels/).
    realtime_should_send = models.BooleanField("Should Send Realtime Notification?")
    realtime_sent = models.BooleanField(
        "Sent Realtime Notification?",
        blank=True,
        null=True,
        default=False,
        help_text=(
            "Did we send the realtime notification?\n"
            "`False` -> We haven't attempted to send yet either because we haven't "
            'gotten to it or because "Should Send Realtime Notification?" was `False`.\n'
            "`True` -> We successfully sent the realtime notification.\n"
            "`None` -> We tried to send the realtime notification but it didn't deliver.\n"
        ),
    )
    realtime_sent_at = models.DateTimeField(
        "Sent Realtime Notification At",
        blank=True,
        null=True,
        default=None,
        help_text=(
            "The time we last attempted (or successfully delivered if "
            '"Sent Realtime Notification?" is `True`) to send a realtime notifciation.'
        ),
    )
    realtime_attempt_count = models.PositiveSmallIntegerField(
        "Realtime Attempt Count",
        default=0,
        help_text=(
            "The number of times we've attempted to send this realtime notification."
        ),
    )
    realtime_error = models.TextField(
        "Realtime Error",
        blank=True,
        default="",
        help_text=(
            "If there was an error trying to send the realtime notification, this will "
            "store that error. At the time of writing, it's an idea that it may or may "
            "not be stored if we later retry and the push notification is/was "
            "successfully delivered."
        ),
    )

    # NOTE: At the time of writing, web push notifications are not currently delivered.
    # Some potential ideas for delivering them:
    #
    # 1. Pusher Beams (https://pusher.com/beams) -> We're already using Pusher so this
    # should be pretty straightforward. Stratbot would probably be able to stay within
    # the free and/or $29/month plan for quite a while as well until ~500-1,000 active
    # concurrent users is exceeded.
    #
    # 2. OneSignal (https://onesignal.com/webpush) -> I think the pricing for scale is
    # cheaper than Pusher Beams (not 100% positive but it initially seemed that way by
    # some significant chunk/degree). Would require a bit of setup with Player IDs and
    # persisting browser sessions on the backend (and deleting when trying to send a
    # push fails because they're no longer active).
    #
    # 3. Django Web Push (https://github.com/safwanrahman/django-webpush) -> This may
    # take a little longer on the development side, but wouldn't cost anything per-month
    # (compared ot the above two options).
    web_push_should_send = models.BooleanField("Should Send Web Push Notification?")
    web_push_sent = models.BooleanField(
        "Sent Web Push Notification?",
        blank=True,
        null=True,
        default=False,
        help_text=(
            "Did we send the web push notification?\n"
            "`False` -> We haven't attempted to send yet either because we haven't "
            'gotten to it or because "Should Send Web Push Notification?" was `False`.\n'
            "`True` -> We successfully sent the web push notification.\n"
            "`None` -> We tried to send the web push notification but it didn't deliver.\n"
        ),
    )
    web_push_sent_at = models.DateTimeField(
        "Sent Web Push Notification At",
        blank=True,
        null=True,
        default=None,
        help_text=(
            "The time we last attempted (or successfully delivered if "
            '"Sent Web Push Notification?" is `True`) to send a web push notifciation.'
        ),
    )
    web_push_attempt_count = models.PositiveSmallIntegerField(
        "Web Push Attempt Count",
        default=0,
        help_text=(
            "The number of times we've attempted to send this web push notification."
        ),
    )
    web_push_error = models.TextField(
        "Web Push Error",
        blank=True,
        default="",
        help_text=(
            "If there was an error trying to send the web push notification, this will "
            "store that error. At the time of writing, it's an idea that it may or may "
            "not be stored if we later retry and the push notification is/was "
            "successfully delivered."
        ),
    )

    created = AutoCreatedField("Created")
    modified = AutoLastModifiedField("Modified")

    class Meta:
        constraints = [
            # We have to give a `name` to the `UniqueConstraint` if we use `fields` with
            # length greater than one. So I do an abbreviated name from
            # {app_abbreviation}__{model_abbreviation}__{field_abbreviations}_abv_uix
            # for the unique constraint.
            models.UniqueConstraint(
                fields=["user", "symbol_rec", "setup", "alert_type"],
                name="al__usa__us_sr_su_at_abv_uix",
            )
        ]
        indexes = [
            # We have to give a `name` to the `Index` if we use `fields` with length
            # greater than one. So I do an abbreviated name from
            # {app_abbreviation}__{model_abbreviation}__{field_abbreviations}_abv_ix for
            # the index.
            models.Index(fields=["user", "created"], name="al__usa__us_ct_abv_ix")
        ]
        verbose_name = "User Setup Alert"
        verbose_name_plural = "User Setup Alerts"

    def __str__(self):
        # NOTE: If converting this to a string ever in the code, just make sure that
        # `select_related("symbol_rec", "user")` was called at some point for optimal
        # performance (see
        # https://docs.djangoproject.com/en/3.2/ref/models/querysets/#select-related).
        return f"{self.symbol_rec.symbol}-related alert for {self.user.email}"


class SetupAlert:
    CANDLE_SHAPES = {
        'hammer': '(h)',
        'shooter': '(s)',
    }

    def __init__(self, symbolrec: SymbolRec, setup: Setup, route: str):
        self.symbolrec = symbolrec
        self.setup = setup
        self.route = route
        self.setup_pattern = '-'.join(setup.pattern)
        # self.candle_shape = '' if self.setup.outside_bar else self.CANDLE_SHAPES.get(setup.df.iloc[-1].candle_shape, '')
        self.candle_shape = ''
        self.display_timeframes = symbolrec.display_timeframes
        self.tfc_timeframes = symbolrec.tfc_timeframes
        self.has_tfc, self.tfc_direction = symbolrec.has_tfc(timeframes=self.tfc_timeframes)
        log.info(f'{route}: {symbolrec.symbol}, {setup}')

    @property
    def prioritized_symbol(self):
        symbol = self.symbolrec.symbol
        # todo: review these conditionals
        if self.setup.direction == self.tfc_direction or (self.setup.is_pmg and self.setup.direction != self.tfc_direction):
            if self.setup.priority == 1:
                match self.route:
                    case 'console': return f'[bright_yellow]{symbol}[/bright_yellow]'
                    case 'discord': return f':large_orange_diamond: {symbol}'
            elif self.setup.priority == 2:
                match self.route:
                    case 'console': return f'[orange1]{symbol}[/orange1]'
                    case 'discord': return f':large_blue_diamond: {symbol}'
        return symbol

    @property
    def timeframes_colored(self) -> str:
        excluded_timeframes = ['6H', '12H']
        output = []
        for tf, state in self.symbolrec.tfc_state(self.display_timeframes).items():
            if tf in excluded_timeframes:
                continue

            if state.distance_ratio > 0:
                match self.route:
                    case 'console': output.append(f'[green]{tf}[/green]')
                    case 'discord': output.append(f'{tf}:green_circle:')
            elif state.distance_ratio < 0:
                match self.route:
                    case 'console': output.append(f'[red]{tf}[/red]')
                    case 'discord': output.append(f'{tf}:red_circle:')
            else:
                match self.route:
                    case 'console': output.append(f'[gray]{tf}[/gray]')
                    case 'discord': output.append(f'{tf}:white_circle:')
        return ' '.join(output)

    @property
    def setup_pattern_colored(self):
        # todo: for console. rewrite (accidentally deleted)
        return self.setup_pattern

    @property
    def strat_text(self):
        match self.route:
            case 'console':
                return f'{self.direction_text} {self.setup_pattern_colored} {self.candle_shape}'
            case 'discord':
                return f'{self.setup_pattern}  {self.candle_shape}'

    @property
    def direction_text(self) -> str:
        if self.setup.direction == 1:
            match self.route:
                case 'console': return '[green]BULL[/green]'
                case 'discord': return ':green_circle: BULL'
        elif self.setup.direction == -1:
            match self.route:
                case 'console': return '[red]BEAR[/red]'
                case 'discord': return ':red_circle: BEAR'
        else:
            return ''

    def directional_value_colored(self, value) -> str:
        if self.setup.direction == 1:
            match self.route:
                case 'console': return f'[green]{value}[/green]'
        elif self.setup.direction == -1:
            match self.route:
                case 'console': return f'[red]{value}[/red]'
        else:
            return str(self.setup.trigger)

    @property
    def pmg_text(self) -> str:
        pmg_candles = self.setup.pmg
        if pmg_candles >= 5:
            match self.route:
                case 'console': return f'[yellow]{pmg_candles}[/yellow]'
                case 'discord': return str(pmg_candles)
        else:
            match self.route:
                # console: '[black]-[/black]'
                case 'console': return ''
                case 'discord': return '-'


class DiscordAlert(SetupAlert):
    TRADINGVIEW_URL = 'https://stratalerts.com/tvr/?symbol={}&interval={}'

    WEBHOOK_URL = 'https://discord.com/api/webhooks'
    ALERT_ROLE = '<@&961700198785118228>'
    PUSH_ROLES = {
        'stock': {
            '60': '<@&976491646126542849>',
            '4H': '<@&976491850720477215>',
            'D': '<@&976491941355225138>',
            'W': '<@&976492132548358214>',
            'M': '<@&976492187141427250>',
            'Q': '<@&976492236613255178>',
            'SSS50': '<@&976493032675364894>',
            'PRIORITY': '<@&976493291262595162>',
        },
        'crypto': {
            '60': '<@&976492365860712469>',
            '4H': '<@&976492417920421938>',
            '6H': '<@&976492476711985254>',
            '12H': '<@&976856265236045884>',
            'D': '<@&976492516905979994>',
            'W': '<@&976492711857242172>',
            'M': '<@&976492756711129108>',
            'Q': '<@&987019256665350174>',
            'SSS50': '<@&976492831428448267>',
            'PRIORITY': '<@&976493355984912434>',
        },
    }

    CRYPTO_ALL = '/942139544474640476/Dki_RaWdff_LY2VJZJUPLCZGp0sQyoyWAkDhsPZ1iUFYMTPyx1zKVyxJO_UsJDe9IPzB'
    CRYPTO_WEBHOOKS = {
        '15': '/942132934129901620/QUKYol7Rlzq6p-_6o34F3LSRRnXXxjs_iRODNBsP--vHpy48SNi9-E87CHltLBu7-H2S',
        '30': '/942140650248347658/zlWj-GluDy4lX8yu3ij0oucuaJfyNZtyuoSyOeq4e_sYUHcWGI8GI9CZ618dx-rsCOS2',
        '60': '/942140760617279568/qfKID_c8OiekZJmHh_0gBm4P3Yr7yiRsdSOO1qEhimgm_QNbZInu3GgvWxYcHdw9GKUP',
        '4H': '/942141018122383380/TSkHPkKCjWsyBXelXCBIL_s9MVrFtcbCW8zO30ny4OKzYV8hpt_Ic75FpDPqeHH7pgPA',
        '6H': '/942141762720395334/5ohYEXnL6oZ04V97tD7sRRTVudM8cMoi2u8Iy4L37uFqVKLJbim-gTtp7SUS3gHTi5BA',
        '12H': '/942141872854409288/rvkILM6M_s_lMFkrRFir0fhJcwlI6OonwTVMS7J5UTSigvy87t1yLqcptS-hQuO15kbF',
        'D': '/942142097916571648/333hk0jBp05gCVGeZiFfqBb6sqHN4k9NUJQqE_cvV-iGqKYJp0XQjsPIPsXrfrXEDsGH',
        'W': '/942142191919317163/_ZAcy3LoZvb2xgorVGhd7XviEkWad3yrhyWVEMXYpqZZDDFYvWJ_RVcprAAzlRzXJhzG',
        'M': '/942142327982551141/lSRZc6DUKbcQvYOG3YRrWCK-SbU2OAza1PvwY-34mEm9nljHT3o17AhVUu3s3buzTbia',
        'Q': '/942142437437083698/bInQGyYudgoMghhz6ZGFPRhdcr1XdZU3Y6GCsxbRYqss3vFVjMz28WjOeDw2MX6y5LM7',
        'Y': '/1178715755269869578/W5fnfKQu3dQLlbGGExcpdxjCsaLdInp_nk2e4oEsh4ykYn4SuEMMK0XtzD3XeTpidgbO'
    }

    STOCKS_ALL = '/942145126740951050/EKnZjdwK5b-ncK5kKouu5EoDV0RBpKa491yVnBdxX07HrrelKA15XrFnY5vqOPJyyoNP'
    STOCKS_WEBHOOKS = {
        '15': '/942145215169437736/ftyIHYvsZ3x8NS51HjjwKLoSuRcZzOqaHGnSIazOSsohaE143nfocw9_P6XAwzIFdS3Y',
        '30': '/942145313886572545/7QzDa4rLsgO3GK1UQdcDeK-HtN_2HnwDVzQQv9N236LG3Es0MpJPDDbg_vzlhyCH117g',
        '60': '/942145404697452545/1nDV83XIrheLOJPdoMM_PrO2J8IoFbgmDFNIqFbTyhII8n7JAhFdwXSnJfomsORn43Qs',
        '4H': '/942145497148317706/6bNoNsVekbvxr-YcfERdobxdISyCRak3OSBr_mgvG8XxjL5kQzyShvJ2z1mgQW88r_wk',
        'D': '/942145623778553886/Uf5wKWwRqLOKOErKRLwJkpbXvWByNcx2va2YvpsTl17FZ-m3HQFd8AS0VckifmJ7G4_b',
        'W': '/942145935394361414/4aXYWdEqmjIXUI553wsxt8E-TtEowhJL01nTKcIP5zEA-O62keKN104o0Rq6xrjKybFf',
        'M': '/942146044962164746/awNSJ3JN88wZOhf4hBdbaTY2GnFsnm15blWXQTJHYZyicjvcPA1yhsAvQGVsFx91qqHG',
        'Q': '/942146190462578728/n23pZ_iWZIRa_Cvxh8OCaga_KtavTqHtkjveCpH3pr5dsorsR8PsuFAJz9HUxGYJ78rH',
        'Y': '/1178715536536911902/-8al5LqvpXgJoRx8fPIbEgS--ZJgolKGwWF9K61iwy2RC6_gzbQ1Gqw_z2t4NqmjnTkK'
    }

    FUTURES_ALL = '/1008531055696953374/_t5Vi9d7XBfyeNCt-MPzmj84oU_hmLaPMt6YqEWK2yi2aexCi5WbQGCp1D3d41vI4jcO'
    FUTURES_WEBHOOKS = {
        '15': '/1008531586989441044/duRE1xdmO5iivRtDndzpnfqKkHvee7uDoG0JIuTc_XaOP5L0Lwz7OQ3A1j0PKEvluT4J',
        '30': '/1008531689246560366/O71egZ7tfbfMoDztxGcVZWh1Os840aAa90bFDm2aYlC1PaH3If0r8XcW0NpfGzpQqNqG',
        '60': '/1008531773598216324/1-w773E36VlUeF_pT997v1uKb9sN592NjP-ILbRdqQOnXap85RCkAnQdg8F26PqOPy3o',
        '4H': '/1008531906465370182/eG7K0Qv4Sps4Ue-XLdr7ECGbnhdmgFfylU2YcPqPsYsh_g507HXEev9BZgbPZ9f9_6AJ',
        '12H': '/1008532044248252416/qizXT85uzLTgiHDEWeQTBJxqqtE-MWiq82nVsnR0MiyRZLyow9Bh_nzLLZiuchMFTTdB',
        'D': '/1008532202553880657/xMO71AiQIsG-UP7fTW4N4nLH_oagCQ07xJca60-VNUPkzE6VI40RQ_r46d3hWjQIq88o',
        'W': '/1008532807926161508/IUphLwAYa8NgfaT3PEn3a-vpcTYDMm1AhjNJZbgyTvYRACaDbntLpdEtD6Z5Tn7T8B2B',
        'M': '/1008532919473680464/4nuyYVzCfQT-g5MP7VnhSDgsaoSaftXDIkiRBA9IXE6ZainH5fNIum23qhOSorxn3jIl',
        'Q': '/1008533399939579985/y5dAAhALCC1YPHnyaPxL2cX8kn_6q1LWCZoK6cTjDDc3shtrtEipj1SP5tScGydeH3TX',
    }

    PRIORITY_CHANNELS = {
        'stock': '/958752308735377468/BGdmjEUttBXjZVo-i-eUl_FqZKqLJSyn3eawfboylcna70O6HUVjT7yv33fX76HFQqKu',
        'crypto': '/958753952734797894/wefiiPb0KPdicmBYGqizVQ-HOXbCU1WRB4X2GzfxfDdoS2UUdkRVXd5uAwFaJaEj69rR',
        'futures': '/1008533521029156885/sTGuvkXDEbrqVh90tLJJYUpEpjzOSfL-6fRcVsq-atOBtYG2wzGhduDTP1IqAFY6wraP',
    }

    MAGNITUDE_CHANNELS = {
        'stock': '/961379898109341736/F46IldwR_9E_uULt-OD_0VBv9gourBAxwf_f_V_bY60bqZuE-Kr-RulSKwWzZqmf8HqQ',
        'crypto': '/961379535964753940/AxCE1fHtWREYgMPOYDMFQk78aUfbh6RE3fWIytPx42zo7XhaPCz4mJ-lz_O3qj-ceAtN',
        'futures': '/1008534626412806185/NxDnW7QZ7n9aqcnWDiZcPGTnJv7VGCZ6Aq_PEZQsh2E0T2e0DPo2dNh6vzTFbCnSjZ7w',
    }

    SSS50_CHANNELS = {
        'stock': '/968871899167875152/1Ca5Qtdusvq4jruDEBr4mVaeOlELWNjWA124wv7ou-Ig7qFySclbsCN_Bm7dbDK8ZNfL',
        'crypto': '/968874573598691391/VLz6bVI2qhcih25kMF-Pl0rKGUJFlH4AsnTBZMUIIn-GoKa_pMMFnEMEzfi3e1W0LU7m',
        'futures': '/1008534302608326687/QE-8KSx0-99Urlrl0wRtWZQGzYPy3RHduxtlImXl5yk9VLW7932gzbtXNnblwxa_rNtv',
    }

    STOCKS_INDEX_CHANNEL = '/1200137475390189671/wwTIJgnKuX7grChSfOpn41ogHRZjxLxRVyuh86ZeCcc8cjSgLE4LIwTS9M8AHhtuJD85'
    STOCKS_GAPPER_CHANNEL = '/1013840156081520680/G3lxAdLAyP_A0EZMriOXtB9VB9b0PqAIWXv9AtQRMIAjvAEBAJx7HNQHM3QbrfH-ECE9'

    PX_SANDBOX = '/944298127131803649/vlxLVXDeIC6w4rsDdHsZqJ5nAfJjZHBCJjB6pmc24pwmUKTx88RpnfGuisFVWJXP_ESk'

    def __init__(self, symbolrec: SymbolRec, setup: Setup):
        super().__init__(symbolrec, setup, 'discord')
        if setup.potential_outside is True:
            self.setup_pattern = 'POTENTIAL 3'
        else:
            two_type = '-2U' if setup.direction == 1 else '-2D'
            if setup.pattern[0] in ['2U', '2D'] and setup.pattern[1] in ['2U', '2D']:
                self.setup_pattern = f'{setup.pattern[-1]}{two_type}'
            else:
                self.setup_pattern = '-'.join(setup.pattern) + two_type

    def _alert_header(self, channel: str, role: str):
        if self.setup.tf in ['15', '30']:
            return ''
        return f'{self.symbolrec.symbol}  [{self.setup.tf}]  {self.setup.bull_or_bear}  {self.strat_text}  {self.PUSH_ROLES[channel][role]}'

    def _create_tradingview_url(self):
        if self.symbolrec.is_crypto:
            url_symbol = f'BINANCE:{self.symbolrec.symbol}.P'
        else:
            url_symbol = self.symbolrec.symbol
        # TODO: better fix for Q to 3M (will also need for Y to 12M)
        match self.setup.tf:
            case 'Q': url_tf = '3M'
            case 'Y': url_tf = '12M'
            case _: url_tf = self.setup.tf
        return self.TRADINGVIEW_URL.format(url_symbol, url_tf)

    def _create_embed(self) -> DiscordEmbed:
        now_utc_short = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        # now_est_short = datetime.now(timezone('America/New_York')).strftime("%Y-%m-%d %H:%M:%S")
        msg_title = f'{self.prioritized_symbol}   [{self.setup.tf}]   {self.strat_text}'
        url = self._create_tradingview_url()
        magnitude_msg = f' :dart: **TARGET HIT**' if self.setup.hit_magnitude else ''
        msg_details = f'```\n' \
                      f'TRIGGER: {self.setup.trigger}\n' \
                      f'   LAST: {self.symbolrec.price}\n' \
                      f'     T1: {self.setup.target}\n' \
                      f'    MAG: {self.setup.magnitude_dollars} ({self.setup.magnitude_percent}%)\n' \
                      f'```{url}\n' \
                      f'{magnitude_msg}\n'
        # f'       RR: {self.setup.rr}\n' \
        # f'      PMG: {self.pmg_text}\n' \
        # f'      UTC: {now_utc_short}\n' \
        # f'  TARGETS: {", ".join([str(t) for t in self.setup.targets[:2]])}\n' \
        # f'  EXPIRES: {setup.expires}\n' \
        color = '0x11ff00' if self.setup.direction == 1 else '0xff0000'
        fields = [
            {
                'name': f'{self.timeframes_colored}',
                'value': msg_details,
            },
        ]
        # https://github.com/lovvskillz/python-discord-webhook
        embed = DiscordEmbed(title=msg_title, color=color, fields=fields)
        embed.set_timestamp()
        return embed

    def _create_url(self, partial_url: str):
        return self.WEBHOOK_URL + partial_url

    def send_msg(self, channel: str):
        """
        Discord messaging
        """
        webhook_channel_map = {
            'stock': self.STOCKS_WEBHOOKS.get(self.setup.tf, []),
            'crypto': self.CRYPTO_WEBHOOKS.get(self.setup.tf, []),
            'px_sandbox': self.PX_SANDBOX,
            'gappers': self.STOCKS_GAPPER_CHANNEL,
        }

        embed = self._create_embed()
        # todo: remove channel check in the future?? used to block dupe mag alerts on dang channels
        if self.setup.hit_magnitude and channel in ['stock', 'crypto']:
            url = self._create_url(self.MAGNITUDE_CHANNELS[channel])
            webhook = DiscordWebhook(url=url, embeds=[embed])
            r = webhook.execute()
            return

        push_role_header = self._alert_header(channel, self.setup.tf)
        url = self._create_url(webhook_channel_map[channel])
        webhook = DiscordWebhook(url=url, content=push_role_header, embeds=[embed])
        r = webhook.execute()

        if channel in ['stock', 'crypto']:
            if self.setup.priority in [1, 2]:
                url = self._create_url(self.PRIORITY_CHANNELS[channel])
                push_role_header = self._alert_header(channel, 'PRIORITY')
                webhook = DiscordWebhook(url=url, content=push_role_header, embeds=[embed])
                r = webhook.execute()

            if 'P3' in self.setup.pattern:
                url = self._create_url(self.SSS50_CHANNELS[channel])
                push_role_header = self._alert_header(channel, 'SSS50')
                webhook = DiscordWebhook(url=url, content=push_role_header, embeds=[embed])
                r = webhook.execute()

            if self.symbolrec.is_index or self.symbolrec.is_sector:
                url = self._create_url(self.STOCKS_INDEX_CHANNEL)
                push_role_header = self._alert_header(channel, 'PRIORITY')
                webhook = DiscordWebhook(url=url, content=push_role_header, embeds=[embed])
                r = webhook.execute()

            partial_url = self.CRYPTO_ALL if self.symbolrec.is_crypto else self.STOCKS_ALL
            url = self._create_url(partial_url)
            webhook = DiscordWebhook(url=url, embeds=[embed])
            r = webhook.execute()


class DiscordGappersAlert:
    WEBHOOK_URL = 'https://discord.com/api/webhooks'
    STOCKS_GAPPER_CHANNEL = '/1013840156081520680/G3lxAdLAyP_A0EZMriOXtB9VB9b0PqAIWXv9AtQRMIAjvAEBAJx7HNQHM3QbrfH-ECE9'

    def __init__(self, direction: int, gappers: list):
        self.gappers = self._sort_gappers_by_percent(gappers)
        self.direction = direction

    @staticmethod
    def _chunk(data: list, n: int) -> list:
        for i in range(0, len(data), n):
            yield data[i:i + n]

    @staticmethod
    def _sort_gappers_by_percent(gappers: list):
        return sorted(gappers, key=lambda x: x.percent)

    def _create_embed(self, gappers: list) -> DiscordEmbed:
        msg_details = ''
        for gapdata in gappers:
            if gapdata.percent > 0.5 or gapdata.percent < -0.5:
                msg_details += f'{gapdata.symbol:5} | {gapdata.last_price:7.2f} | {gapdata.dollars:5.2f} | {gapdata.percent:5.2f}%\n'
        if msg_details:
            msg_details += f'```\n'
            msg_title = f'LEADING GAPPERS'
            msg_header = '```\n' \
                         f'{"SYM":5} | {"LAST":7} | {"GAP $":5} | {"GAP %":5}\n' \
                         '==================================\n'
            color = '0x11ff00' if self.direction == 1 else '0xff0000'
            fields = [
                {
                    'name': msg_title,
                    'value': msg_header + msg_details,
                },
            ]
            # https://github.com/lovvskillz/python-discord-webhook
            embed = DiscordEmbed(color=color, fields=fields)
            embed.set_timestamp()
            return embed

    def send_msg(self):
        for gappers in self._chunk(self.gappers, 25):
            if embed := self._create_embed(gappers):
                url = self.WEBHOOK_URL + self.STOCKS_GAPPER_CHANNEL
                webhook = DiscordWebhook(url=url, embeds=[embed])
                r = webhook.execute()
