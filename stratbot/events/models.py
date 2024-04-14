import pytz
from discord_webhook import DiscordEmbed, DiscordWebhook
from django.db import models


class CalendarEvent(models.Model):
    timestamp = models.DateTimeField()
    currency = models.CharField(max_length=3)
    impact = models.CharField(max_length=32)
    event = models.CharField(max_length=128)
    forecast = models.CharField(max_length=32)
    previous = models.CharField(max_length=32)
    discord_notified = models.BooleanField(default=False)

    class Meta:
        unique_together = ('timestamp', 'currency', 'impact', 'event')

    def __str__(self):
        return f'{self.timestamp} - {self.currency} - {self.event}'

    def send_to_discord(self):
        webhooks = [
            'https://discord.com/api/webhooks/1198779833166073937/AcGd34o9Q0z9hmIIWON5kvcvcn6hdwBNJTOBrbm6oquxQfCcrBJuSN6QlP_qCZFS5tac',
            'https://discord.com/api/webhooks/1198779606073884732/_oosQlckzpOB4k8t5EpagdhA89RDcJERGqHAjxHy1sm4CvUCEpUwdQaucvVWJF4oFSPT',
            'https://discord.com/api/webhooks/1199373337353265152/04doyplur_OZ8H8gSmMIJtimKODvth1SvWt_c3ZrRpsz2PZlAFJ8JON7uBL5k8DJFDsx',
        ]
        title = f'{self.impact.upper()} IMPACT ECONOMIC EVENT IN 15 MINUTES'
        color = '0xffcc99' if self.impact == 'Medium' else '0xffcccc'
        embed = DiscordEmbed(title=title, description=self.event, color=color)
        embed.set_timestamp()
        est = pytz.timezone('US/Eastern')
        embed.add_embed_field(name='Event Time', value=self.timestamp.astimezone(est).strftime('%a, %b %d %-I:%M%p'))
        embed.add_embed_field(name='Previous', value=self.previous)
        embed.add_embed_field(name='Forecast', value=self.forecast)
        # embed.set_footer(text='https://app.stratalerts.com')

        for hook in webhooks:
            webhook = DiscordWebhook(url=hook)
            webhook.add_embed(embed)
            webhook.execute()
