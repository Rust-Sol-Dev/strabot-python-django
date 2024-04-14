import os
from datetime import datetime
from collections import defaultdict, OrderedDict
from time import perf_counter
import hashlib

import django
from django.utils import timezone

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")
django.setup()

from django.db.utils import IntegrityError
import pytz

from stratbot.scanner.models.symbols import SymbolRec, Setup, DominoGroup, DominoGroupMember, Direction
from stratbot.scanner.models.timeframes import Timeframe
from stratbot.scanner.ops.candles.metrics import within_percentage


def generate_hash(setups):
    sorted_setups = sorted(setups, key=lambda x: Timeframe(x.tf))
    setup_str = ''.join([f"{setup.symbol_rec.symbol}:{setup.trigger}:{setup.tf}:{setup.expires}" for setup in sorted_setups])
    return hashlib.sha256(setup_str.encode()).hexdigest()


def get_setups_by_direction():
    setups = (Setup.objects
              .prefetch_related('symbol_rec')
              .filter(expires__gt=timezone.now())
              .filter(symbol_rec__symbol_type='stock')
              .filter(negated=False)
              .filter(hit_magnitude=False)
              .filter(tf__in=['D', 'W'])
              )
    setups_by_direction = defaultdict(lambda: defaultdict(list))
    for setup in setups:
        setups_by_direction[setup.symbol_rec.symbol][setup.direction].append(setup)

    return setups_by_direction


def main():
    setups_by_direction = get_setups_by_direction()
    count = 0
    for symbol, directions_setups_map in setups_by_direction.items():
        # print(symbol, directions_setups_map)
        # print(symbol)
        for setups in directions_setups_map.values():
            # print(setups)
            # setups.reverse()
            # print(setups)

            # for setup in setups:
            #     print(setup)
            # print('===============================================================')

            # unique_setups = OrderedDict()
            # for setup in setups:
            #     if setup.timestamp not in unique_setups.keys():
            #         unique_setups[setup.timestamp] = setup
            #
            # setups = list(unique_setups.values())

            setups.sort(key=lambda x: x.trigger)

            # symbolrec = SymbolRec.objects.get(symbol=symbol)
            symbolrec = setups[0].symbol_rec
            tfc_state = symbolrec.tfc_state(['D', 'W', 'M', 'Q'])
            # visited = set()
            for i, setup1 in enumerate(setups):
                # print(setup1)
                if (tfc_state[setup1.tf].distance_percent > 0 and setup1.direction != Direction.BULL) or (
                        tfc_state[setup1.tf].distance_percent < 0 and setup1.direction != Direction.BEAR
                ):
                    # print(f'skipping [{symbol}] {setup1} because it is not in the direction of TFC')
                    continue

                # if i in visited:
                #     continue
                domino_setups = [setup1]

                for j in range(i + 1, len(setups)):  # Start j at i + 1 to avoid comparing setup with itself
                    setup2 = setups[j]
                    if within_percentage(setup1.trigger, setup2.trigger, 0.5):
                        domino_setups.append(setup2)
                        # visited.add(j)

                if len(domino_setups) > 1:
                    domino_setups = sorted(domino_setups, key=lambda x: Timeframe(x.tf))
                    symbol_rec = domino_setups[0].symbol_rec
                    expires = domino_setups[0].expires
                    direction = domino_setups[0].direction
                    unique_hash = generate_hash(domino_setups)
                    try:
                        # domino_group = DominoGroup.objects.create(
                        #     symbol_rec=symbol_rec,
                        #     direction=direction,
                        #     unique_hash=unique_hash,
                        #     expires=expires,
                        #     # skip_duplicates=True,
                        # )
                        pass
                    except IntegrityError as e:
                        print(f'skipping duplicate: {e}')
                    else:
                        print('===============================================================')
                        print(f"Potential domino setups for [{symbol}]")
                        print('---------------------------------------------------------------')
                        count += 1
                        for setup in domino_setups:
                            print(f" - {setup}")
                            # DominoGroupMember.objects.create(
                            #     domino_group=domino_group,
                            #     setup=setup,
                            # )
                        print()
    print(f'Found {count} potential domino setups')


main()

# unique_setups = OrderedDict()
# for setup in setups:
#     if setup.trigger not in unique_setups:
#         unique_setups[setup.trigger] = setup

# Check for "domino" trades
# checked = set()
# for i, setup1 in enumerate(setups):
#     if i in checked:
#         continue
#     domino_setups = [setup1]
#     for j, setup2 in enumerate(setups):
#         if i != j and within_percentage(setup1.trigger, setup2.trigger):
#             domino_setups.append(setup2)
#             checked.add(j)
#     if len(domino_setups) > 1:
#         print(f"Potential domino trade for symbol {symbol}, direction {direction}:")
#         for setup in domino_setups:
#             print(f" - Setup with trigger: {setup.tf} - {setup.trigger} - {setup}")


# dgs = DominoGroup.objects.filter(symbol_rec__symbol='NVDA')
# for group in dgs:
#     members = group.dominogroupmember_set.all()
#     # members = sorted(members, key=lambda x: Timeframe(x.tf))
#     d_setups = []
#     for member in members:
#         d_setups.append(member.setup)
#     d_setups = sorted(d_setups, key=lambda x: Timeframe(x.tf))
#     for setup in d_setups:
#         print(setup, setup.symbol_rec.symbol,
#               setup.trigger)  # Assuming the DominoGroupMembership model has a 'setup' field
#     print('===============================================================')
