import pytz
import requests
from io import BytesIO
from io import StringIO
import csv
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from discord_webhook import DiscordWebhook, DiscordEmbed

from stratbot.events.models import CalendarEvent


class ForexFactory:

    CSV_URL = 'https://nfs.faireconomy.media/ff_calendar_thisweek.csv'

    def __init__(self):
        self.url = self.CSV_URL
        self.df = pd.DataFrame()
        self.buffer = BytesIO()

    def scrape(self) -> None:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        response = requests.get(self.url, headers=headers)
        response.raise_for_status()
        data = []
        if response.status_code != 200:
            raise Exception(f'error: {response.status_code=}, {response.text=}')
        csv_content = StringIO(response.text)

        reader = csv.reader(csv_content)
        next(reader)
        for row in reader:
            date_object = datetime.strptime(row[2], '%m-%d-%Y').date()
            time_object = datetime.strptime(row[3], '%I:%M%p').time()
            combined_object = datetime.combine(date_object, time_object)

            utc = pytz.timezone('UTC')
            localized_datetime = utc.localize(combined_object)

            p = {
                "timestamp": localized_datetime,
                "currency": row[1],
                "impact": row[4],
                "event": row[0],
                "forecast": row[5],
                "previous": row[6]
            }
            data.append(p)
        csv_content.close()
        self.df = pd.DataFrame(data)

    def plot(self, df: pd.DataFrame = None):
        plt.figure(figsize=(10, 3))
        plt.rcParams.update({'font.size': 10})

        ax = plt.gca()
        ax.axis('off')
        tbl = ax.table(cellText=df.values, colLabels=df.columns, loc='center', cellLoc='center')

        tbl.auto_set_font_size(False)
        tbl.set_fontsize(14)
        tbl.auto_set_column_width(col=list(range(len(df.columns))))

        for pos, cell in tbl.get_celld().items():
            cell.set_height(0.2)
            if pos[0] == 0:
                cell.set_facecolor('lightgray')
            if 'High' in cell.get_text().get_text():
                cell.set_facecolor('#ffcccc')
            elif 'Medium' in cell.get_text().get_text():
                cell.set_facecolor('#ffcc99')

        plt.savefig(self.buffer, format='png', bbox_inches='tight', pad_inches=0.1)
        self.buffer.seek(0)

    def send_to_discord(self):
        if (isinstance(self.df, pd.DataFrame) and self.df.empty) or self.df is None:
            self.scrape()

        df = self.df.copy()
        est = pytz.timezone('US/Eastern')
        df['timestamp'] = df['timestamp'].dt.tz_convert(est)
        df['date'] = df['timestamp'].dt.strftime('%a, %b %d')
        df['day'] = df['timestamp'].dt.day_name()
        df['time'] = df['timestamp'].dt.strftime("%-I:%M%p")
        del df['timestamp']
        df = df[df['currency'] == 'USD']
        df = df[['date', 'day', 'time', 'impact', 'event', 'forecast', 'previous']]

        # add blank spaces between days
        df['new_day'] = df['day'].ne(df['day'].shift())
        new_day_indexes = df.index[df['new_day']]
        blank_row = pd.DataFrame({col: '' for col in df.columns}, index=[i - 0.5 for i in new_day_indexes])
        df = pd.concat([df, blank_row]).sort_index().reset_index(drop=True)
        df.drop(['day', 'new_day'], axis=1, inplace=True)

        df.columns = [col[0].upper() + col[1:] for col in df.columns]
        self.plot(df)

        webhook_url = 'https://discord.com/api/webhooks/985682501391491123/eMOioyOP0TgXXNfOwjAgLsLTKPYTjOLnQfRDsTEGOqE1wWPeXYa2irxfGmn8akYTQ_j1'
        webhook = DiscordWebhook(url=webhook_url)
        embed = DiscordEmbed()
        embed.set_image(url='attachment://dataframe.png')
        webhook.add_embed(embed)
        webhook.add_file(file=self.buffer.read(), filename='dataframe.png')
        response = webhook.execute()

    def df_to_db(self):
        if (isinstance(self.df, pd.DataFrame) and self.df.empty) or self.df is None:
            self.scrape()

        df = self.df.copy()
        model_instances = [CalendarEvent(**row) for row in df.to_dict('records')]
        CalendarEvent.objects.bulk_create(model_instances, ignore_conflicts=True)
