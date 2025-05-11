import model


def test_base64_decode():
    base64_input = read_text('data.b64')

    actual_result = model.base64_decode(base64_input)
    expected_result = {
        "ride": {
            "PULocationID": 130,
            "DOLocationID": 205,
            "trip_distance": 3.66,
        },
        "ride_id": 256,
    }

    assert actual_result == expected_result

def test_prepare_features():
    model_service = model.ModelService(None)
    ride = {
        'PULocationID': 130,
        'DOLocationID': 205,
        'trip_distance': 3.66
    }
    expected_features = {
        'PU_DO': '130_205',
        'trip_distance': 3.66
    }
    actual_features = model_service.prepare_features(ride)
    assert actual_features == expected_features, f"Expected {expected_features}, but got {actual_features}"