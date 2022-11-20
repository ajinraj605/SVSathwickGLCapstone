from geopy import distance

def get_distance(user_co, friend_co):
    coords_1 = (user_co.get('lat'), user_co.get('long'))
    coords_2 = (float(friend_co.get('lat')), float(friend_co.get('long')))

    return distance.distance(coords_1, coords_2).km


user = {'lat': 25.276987, 'long': 55.296249}
friends= {  'user2':{ 'lat':'25.276987','long':'55.295249'},
            'user3':{ 'lat':'25.276987','long':'55.284249'},
            'user4':{ 'lat':'25.276987','long':'55.273249'},
            'user5':{ 'lat':'25.276987','long':'55.262249'},
            'user6':{ 'lat':'25.276987','long':'55.251249'}
         }

for key, value in friends.items():    
    dstnce =  get_distance(user, value)
    print(f"""Distance between user1 and {key}: {dstnce} """)