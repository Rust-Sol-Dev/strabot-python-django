from datetime import datetime, timezone
import logging

import pytz
from discord_webhook import DiscordWebhook, DiscordEmbed
from rich.table import Table

from stratbot.scanner.models.symbols import SymbolRec, Direction

from .setups import SetupMsg


log = logging.getLogger(__name__)


class SetupMsgAlert:
    CANDLE_SHAPES = {
        'hammer': '(h)',
        'shooter': '(s)',
    }

    def __init__(self, symbolrec: SymbolRec, setup: SetupMsg, route: str):
        self.symbolrec = symbolrec
        self.setup = setup
        self.route = route
        self.setup_pattern = '-'.join(setup.pattern)
        self.display_timeframes = symbolrec.display_timeframes
        self.tfc_timeframes = symbolrec.tfc_timeframes
        self.candle_shape = self.CANDLE_SHAPES.get(setup.shape, '')

        # self.has_tfc, self.tfc_direction = symbolrec.has_tfc(timeframes=self.tfc_timeframes)
        # log.info(f'{route}: {symbolrec.symbol}, {setup}')

    # @property
    # def prioritized_symbol(self):
    #     symbol = self.symbolrec.symbol
    #     # todo: review these conditionals
    #     if self.setup.direction == self.tfc_direction or (self.setup.is_pmg and self.setup.direction != self.tfc_direction):
    #         if self.setup.priority == 1:
    #             match self.route:
    #                 case 'console': return f'[bright_yellow]{symbol}[/bright_yellow]'
    #                 case 'discord': return f':large_orange_diamond: {symbol}'
    #         elif self.setup.priority == 2:
    #             match self.route:
    #                 case 'console': return f'[orange1]{symbol}[/orange1]'
    #                 case 'discord': return f':large_blue_diamond: {symbol}'
    #     return symbol

    @property
    def timeframes_colored(self) -> str:
        excluded_timeframes = []
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


class DiscordMsgAlert(SetupMsgAlert):
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
            'Y': '',
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
            'Y': '',
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
    # STOCKS_GAPPER_CHANNEL = '/1013840156081520680/G3lxAdLAyP_A0EZMriOXtB9VB9b0PqAIWXv9AtQRMIAjvAEBAJx7HNQHM3QbrfH-ECE9'
    STOCKS_GAPPER_CHANNEL = '/1212813643532738590/OYWx_r1RH4YR2lBMFwRqNygSmpxuJwd9TJsuSmKapwDyk9y3939mbnDMJzjhYeHfU2Tr'
    STOCKS_EXPANDO_CHANNEL = '/1217310659906240584/iMbzA1Eghlgvsz0FseiMSa1bAIi0MqSm4BNXw9LNlxRsMIncpucjwHHFN65mZf2c7GGN'
    STOCKS_FTFC_CHANNEL = '/1217689179023216700/UznH3lyYb29NkwqpSpUXwv8km_KcarwryR0qblJMyEvqKM1p3Vha15rvwq3l9JeKeWEf'
    STOCKS_REVSTRATS_CHANNEL = '/1220900805633245254/Eew6zDNFVE93-P-0xt0zEJE5Xlc9OUb1xX_xZtx-NQc4Fc93V5d-DvKIaUp376sVA74t'

    CRYPTO_EXPANDO_CHANNEL = '/1217310529056538774/udiDr-4yzSZATU3zdeDLSLlvsEzGJvTYcWJtq-CgJcSmYdyXwuP1GTFDtEub3N0mCQ31'
    CRYPTO_FTFC_CHANNEL = '/1217672185565413468/1jWulFYj5hRyDaSQYsjzZaLFFSECBwuf5Owfui6Hk5yQm1t2q9qAOsqGXi1NdiNkxoNk'
    CRYPTO_REVSTRATS_CHANNEL = '/1220899886904377376/gr9HFSax-HR9jKKTud_wsfLAqkiYk03DuMrZleRteNZ0HlTmdKD1tUiCsJcViqEwN2CY'

    def __init__(self, symbolrec: SymbolRec, setup: SetupMsg):
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
        try:
            push_role = self.PUSH_ROLES[channel][role]
        except KeyError:
            push_role = ''
        header = f'{self.symbolrec.symbol}  [{self.setup.tf}]  {Direction(self.setup.direction).label}  {self.strat_text}  {push_role}'
        return header

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
        # msg_title = f'{self.prioritized_symbol}   [{self.setup.tf}]   {self.strat_text}'
        msg_title = f'{self.symbolrec.symbol}   [{self.setup.tf}]   {self.strat_text}'
        url = self._create_tradingview_url()
        notes_msg = self.setup.notes
        if self.setup.notes:
            notes_msg = f':warning: {self.setup.notes}'
        magnitude_msg = f' :dart: **TARGET HIT**' if self.setup.hit_magnitude else ''
        msg_details = f'```\n' \
                      f'TRIGGER: {self.setup.trigger}\n' \
                      f'   LAST: {self.symbolrec.price}\n' \
                      f'     T1: {self.setup.target}\n' \
                      f'    MAG: {self.setup.magnitude_dollars} ({self.setup.magnitude_percent}%)\n' \
                      f'```{url}\n' \
                      f'{magnitude_msg}\n' \
                      f'{notes_msg}\n'
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
        # return 'https://discord.com/api/webhooks/1209649164422742137/hJAgZ9eDujpt8-_aILvrU_2ZpxF9-B9CnkBRiadI4XqdoIE7OP_kZtW48yAu-9QrC7MC'

    def send_msg(self, channel: str):
        webhook_channel_map = {
            'stock': self.STOCKS_WEBHOOKS.get(self.setup.tf, []),
            'gappers': self.STOCKS_GAPPER_CHANNEL,
            'stock-expando': self.STOCKS_EXPANDO_CHANNEL,
            'stock-ftfc': self.STOCKS_FTFC_CHANNEL,
            'stock-revstrats': self.STOCKS_REVSTRATS_CHANNEL,
            'crypto': self.CRYPTO_WEBHOOKS.get(self.setup.tf, []),
            'crypto-expando': self.CRYPTO_EXPANDO_CHANNEL,
            'crypto-ftfc': self.CRYPTO_FTFC_CHANNEL,
            'crypto-revstrats': self.CRYPTO_REVSTRATS_CHANNEL,
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
        # webhook = DiscordWebhook(url=url, embeds=[embed])
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


def create_table():
    now_utc_short = datetime.now(tz=pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
    now_est_short = datetime.now(pytz.timezone('America/New_York')).strftime("%Y-%m-%d %H:%M:%S")

    table = Table(title=f'{now_utc_short} UTC ({now_est_short} EST)')
    table.add_column('SYMBOL', width=12)
    # table.add_column('TFC', width=20)
    table.add_column('TF', width=10)
    table.add_column('DIR', width=10)
    table.add_column('STRAT', width=10)
    table.add_column('TRIGGER', width=10)
    table.add_column('LAST', width=10)
    table.add_column('T1', width=10)
    table.add_column('HIT MAG', width=7)

    return table


def add_row(table: Table, setup: SetupMsg) -> Table:
    # alert = SetupMsgAlert(setup, route='console')
    table.add_row(
        setup.symbol,
        # timeframes_colored(setup.tfc_table),
        setup.tf,
        setup.bull_or_bear,
        '-'.join(setup.pattern),
        str(setup.trigger),
        str(setup.current_bar.c),
        str(setup.target),
        '[yellow]#[/yellow]' if setup.hit_magnitude else '-',
    )
    return table
