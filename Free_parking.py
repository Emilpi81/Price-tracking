




parking_garage = {
    'floor1': ['free', 'full'],
    'floor2': ['free', 'full','full'],
    'floor3': ['free', 'full', 'free']
}

def count_free_spots(parking_garage):
    free_spots_per_floor = {}
    for floor, spots in parking_garage.items():
        free_count = spots.count('free')
        free_spots_per_floor[floor] = free_count
    return free_spots_per_floor

free_spots = count_free_spots(parking_garage)

for floor, free_count in free_spots.items():
    print(f'{floor}: {free_count} free spots')

