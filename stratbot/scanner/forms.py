from __future__ import annotations

from crispy_forms.helper import FormHelper
from crispy_forms.layout import Column, Div, Fieldset, Layout, HTML, Reset, Row
from crispy_tailwind.layout import Reset, Submit, Button
from django import forms

from stratbot.scanner.models.symbols import Direction, SymbolType
from stratbot.scanner.models.timeframes import Timeframe


class SetupFilterForm(forms.Form):

    symbol_type = forms.ChoiceField(
        initial="stock",
        required=True,
        # choices=[
        #     ("", "-----"),
        # ]
        choices=SymbolType.choices,
    )

    tf = forms.ChoiceField(
        label="Timeframe",
        initial="D",
        # choices=[
        #     ("all", "-----"),
        # ]
        choices=Timeframe.choices[2:],
        required=False,
        # widget=forms.HiddenInput(),
    )

    pattern = forms.ChoiceField(
        label="Pattern",
        choices=(
            ("", "-----"),
            ("x-1", "x-1-x"),
            ("1-1", "1-1-x"),
            ("1-2", "1-2-x"),
            # ("1-3", "1-3-×"),
            ("2-1", "2-1-x"),
            ("2-2", "2-2-x"),
            # ("2-3", "2-3-×"),
            ("3-1", "3-1-x"),
            ("3-2", "3-2-x"),
            ("x-3", "x-3-x"),
            ("1-3", "1-3-x"),
        ),
        required=False,
        initial="",
    )

    current_candle = forms.ChoiceField(
        label="CC",
        choices=(
            ("", "-----"),
            ("1", "1"),
            ("2U", "2U"),
            ("2D", "2D"),
            ("3", "3"),
        ),
        required=False,
        initial="",
    )

    tfc = forms.ChoiceField(
        label="FTFC",
        choices=(
            ("", "-----"),
            ("BULL", "BULL"),
            ("BEAR", "BEAR"),
        ),
        required=False,
        initial="",
    )

    candle_tag = forms.ChoiceField(
        label="Tags",
        choices=(
            ("", "-----"),
            ("2d green hammer", "2d green hammer"),
            ("2u red shooter", "2u red shooter"),
            ("hammer", "hammer"),
            ("shooter", "shooter"),
            ("hammer OR shooter", "hammer OR shooter"),
            ("hammer shooter", "hammer + shooter"),
            ("shooter hammer", "shooter + hammer"),
            ("2d green", "2d green"),
            ("2u red", "2u red"),
        ),
        required=False,
        initial="",
    )

    direction = forms.ChoiceField(
        label="Direction",
        choices=[
            (0, "-----")
        ] + Direction.choices,
        required=False,
        initial="",
    )

    in_force = forms.ChoiceField(
        label="In Force",
        choices=[
            ("", "-----"),
            (True, "Yes"),
            (False, "No"),
        ],
        required=False,
        initial="",
    )

    hit_magnitude = forms.ChoiceField(
        label="Hit Mag",
        choices=[
            ("", "-----"),
            (True, "Yes"),
            (False, "No"),
        ],
        required=False,
        initial=False,
    )

    potential_outside = forms.ChoiceField(
        label="P3",
        choices=(
            ("", "-----"),
            (True, "Yes"),
            (False, "No"),
        ),
        required=False,
        initial="",
    )

    rr__gte = forms.DecimalField(
        label="RR Min",
        widget=forms.NumberInput(
            attrs={
                'class': 'numberinput form-input',
                'style': 'width: 5em;'
            }
        ),
        required=False,
        initial=0,
    )

    pmg__gte = forms.IntegerField(
        label="PMG",
        initial=0,
        required=False,
        widget=forms.NumberInput(
            attrs={
                'class': 'numberinput form-input',
                'style': 'width: 5em;',
            },
        ),
    )

    gapped = forms.ChoiceField(
        label="Gappers",
        choices=(
            ("", "-----"),
            (True, "Yes"),
            (False, "No"),
        ),
        required=False,
        initial="",
    )

    negated = forms.ChoiceField(
        label="Negated",
        choices=(
            ("", "-----"),
            (True, "Yes"),
            (False, "No"),
        ),
        required=False,
        initial=False,
    )

    symbol_rec__atr__gte = forms.FloatField(
        label="ATR $",
        required=False,
        widget=forms.NumberInput(
            attrs={
                'class': 'numberinput form-input',
                'style': 'width: 5em;',
            },
        ),
        initial=0,
    )

    symbol_rec__atr_percentage__gte = forms.FloatField(
        label="ATR %",
        required=False,
        widget=forms.NumberInput(
            attrs={
                'class': 'numberinput form-input',
                'style': 'width: 5em;',
            },
        ),
        initial=0,
    )

    min_price = forms.IntegerField(
        label="Min Price",
        widget=forms.NumberInput(attrs={
            'class': 'numberinput form-input',
            'style': 'width: 5em;',
        }),
        required=False,
        initial=0,
    )

    max_price = forms.IntegerField(
        label="Max Price",
        widget=forms.NumberInput(attrs={
            'class': 'numberinput form-input',
            'style': 'width: 5em;',
        }),
        required=False,
    )

    sector = forms.ChoiceField(
        label="Sector",
        choices=(
            ("", "-----"),
            ('communication-services', 'Communications'),
            ('consumer-cyclical', 'Consumer Cyclical'),
            ('consumer-defensive', 'Consumer Defensive'),
            ('energy', 'Energy'),
            ('financial-services', 'Financial'),
            ('real-estate', 'Real Estate'),
            ('healthcare', 'Healthcare'),
            ('technology', 'Tech'),
            ('basic-materials', 'Materials'),
            ('industrials', 'Industrials'),
            ('utilities', 'Utilities'),
        ),
        required=False,
        initial="",
    )

    industry = forms.CharField(
        label="Industry",
        required=False,
        widget=forms.TextInput(
            attrs={
                'class': 'numberinput form-input',
                'style': 'width: 8em;',
            },
        ),
    )

    market_cap = forms.ChoiceField(
        label="Market Cap",
        choices=(
            (0, "-----"),
            (1000000000, '1B+'),
            (5000000000, '5B+'),
            (10000000000, '10B+'),
            (50000000000, '50B+'),
            (100000000000, '100B+'),
            (500000000000, '500B+'),
            (1000000000000, '1T+'),
        ),
        required=False,
        initial="50000000000",
    )

    def __init__(self, *args, request=None, is_stock=False, **kwargs):
        super().__init__(*args, **kwargs)

        self.helper = FormHelper(self)
        self.helper.form_method = "GET"
        self.helper.form_id = "setup-filter-form-id"

        primary_row_layout = [
            Column("tf"),
            Column("pattern", css_class="px-1"),
            Column("candle_tag", css_class="px-1"),
            Column("direction", css_class="px-1"),
            Column("in_force", css_class="px-1"),
            Column("hit_magnitude", css_class="px-1"),
            Column("current_candle", css_class="px-1"),
            Column("tfc", css_class="px-1"),
            Column("pmg__gte", css_class="px-1"),
            Column(
                "potential_outside",
                data_toggle="tooltip",
                data_placement="top",
                title="SSS50% Rule Triggered",
                css_class="flex flex-wrap -mx-1"
            ),
        ]

        secondary_row_layout = [
            Column("symbol_rec__atr__gte", css_class="px-1"),
            Column("symbol_rec__atr_percentage__gte", css_class="px-1"),
            Column("min_price", css_class="px-1"),
            Column("max_price", css_class="px-1"),
            Column(
                "rr__gte",
                css_class="px-1",
                data_toggle="tooltip",
                data_placement="top",
                title="Risk/Return expresssed in Rs"
            ),
        ]

        if is_stock:
            choices = []
            for timeframe in Timeframe.choices:
                if timeframe[0] not in ('1', '5', '6H', '12H'):
                    choices.append(timeframe)
            # self.fields['tf'].choices = [
            #     ("all", "-----"),
            # ] + choices[2:]
            self.fields['tf'].choices = choices

            self.fields['market_cap'] = forms.ChoiceField(
                label="Market Cap",
                choices=(
                    (0, "-----"),
                    (1000000000, '1B+'),
                    (5000000000, '5B+'),
                    (10000000000, '10B+'),
                    (50000000000, '50B+'),
                    (100000000000, '100B+'),
                    (500000000000, '500B+'),
                    (1000000000000, '1T+'),
                ),
                required=False,
                initial="50000000000",
            )
            secondary_row_layout.append(Column("market_cap", css_class="px-1"))

            self.fields['spread'] = forms.ChoiceField(
                label="Spread",
                choices=(
                    ("", "-----"),
                    (0.10, "< 0.10%"),
                    (0.25, "< 0.25%"),
                    (0.5, "< 0.5%"),
                    (1, "< 1.0%"),
                    (2, "< 2.0%"),
                    (3, "< 3.0%"),
                    (4, "< 4.0%"),
                    (5, "< 5.0%"),
                ),
                required=False,
                initial="",
            )
            secondary_row_layout.append(Column("spread", css_class="px-1"))

            self.fields['sector'] = forms.ChoiceField(
                label="Sector",
                choices=(
                    ("", "-----"),
                    ('communication-services', 'Communications'),
                    ('consumer-cyclical', 'Consumer Cyclical'),
                    ('consumer-defensive', 'Consumer Defensive'),
                    ('energy', 'Energy'),
                    ('financial-services', 'Financial'),
                    ('real-estate', 'Real Estate'),
                    ('healthcare', 'Healthcare'),
                    ('technology', 'Tech'),
                    ('basic-materials', 'Materials'),
                    ('industrials', 'Industrials'),
                    ('utilities', 'Utilities'),
                ),
                required=False,
                initial="",
            )
            secondary_row_layout.append(Column("sector", css_class="px-1"))

            # self.fields['industry'] = forms.CharField(
            #     label="Industry",
            #     required=False,
            #     widget=forms.TextInput(
            #         attrs={
            #             'class': 'numberinput form-input',
            #             'style': 'width: 8em;',
            #         },
            #     ),
            # )
            # row_layout.insert(12, Column("industry", css_class="px-1"))

            self.fields['gapped'] = forms.ChoiceField(
                label="Gappers",
                choices=(
                    ("", "-----"),
                    (True, "Yes"),
                    (False, "No"),
                ),
                required=False,
                initial="",
            )
            secondary_row_layout.append(Column("gapped", css_class="px-1"))

        if request and request.user.is_superuser:
            self.fields['negated'] = forms.ChoiceField(
                label="Negated",
                choices=(
                    ("", "-----"),
                    (True, "Yes"),
                    (False, "No"),
                ),
                required=False,
                initial=False,
            )
            secondary_row_layout.append(Column("negated"))

        self.helper.layout = Layout(
            Row(*primary_row_layout, css_class="w-full md:w-1/2 lg:w-1/3 flex flex-wrap -mx-1"),
            Row(*secondary_row_layout, css_class="w-full md:w-1/2 lg:w-1/3 flex flex-wrap -mx-1"),
            Submit("filter_button", "Filter",
                   css_class="p-2 text-xs font-medium text-center text-white bg-primary-700 rounded-lg hover:bg-blue-800 focus:ring-4 focus:outline-none focus:ring-blue-300 dark:bg-blue-600 dark:hover:bg-blue-700 dark:focus:ring-blue-800"),
            # Reset("reset", "Reset",
            #       css_class="px-3 py-2 text-xs font-medium text-center text-white bg-red-700 rounded-lg hover:bg-red-800 focus:ring-4 focus:outline-none focus:ring-red-300 dark:bg-red-600 dark:hover:bg-red-700 dark:focus:ring-red-800"),
            HTML("""
                    <button id="updateFilterButton" data-modal-toggle="updateFilterModal" class="p-2 text-xs font-medium text-center text-white bg-primary-700 rounded-lg hover:bg-blue-800 focus:ring-4 focus:outline-none focus:ring-blue-300 dark:bg-blue-600 dark:hover:bg-blue-700 dark:focus:ring-blue-800" type="button">
                        Save
                    </button>
                """),
            # Submit("save_button", "Save",
            #        css_class="p-2 text-xs font-medium text-center text-white bg-primary-700 rounded-lg hover:bg-blue-800 focus:ring-4 focus:outline-none focus:ring-blue-300 dark:bg-blue-600 dark:hover:bg-blue-700 dark:focus:ring-blue-800"),
            HTML('<a href="{{ request.path }}" class="button p-2 mr-1 text-xs font-medium text-center text-white bg-red-600 rounded-lg hover:bg-red-800 focus:ring-4 focus:outline-none focus:ring-red-300 dark:bg-red-500 dark:hover:bg-red-700 dark:focus:ring-red-800">Reset</a>'),
        )
