import pickle
import mlflow

from mlflow.tracking import MlflowClient
from flask import Flask, request, jsonify

# MLFLOW_TRACKING_URI = 'http://127.0.0.1:5000'
RUN_ID = "05eb31b5ce63488bb169e5c00f50e241"

# mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

logged_model = f'mlartifacts/1/{RUN_ID}/artifacts/model'
# logged_model = f'runs:/{RUN_ID}/model'
model = mlflow.pyfunc.load_model(logged_model)

def prepare_features(ride):
    features = {}
    features['PU_DO'] = f"{ride['PULocationID']}_{ride['DOLocationID']}"
    features['trip_distance'] = ride['trip_distance']
    return features


def predict(features):
    preds = model.predict(features)
    return preds[0]


app = Flask('duration-prediction')


@app.route('/predict', methods=['POST'])
def predict_endpoint():
    ride  = request.get_json()

    features = prepare_features(ride)
    pred = predict(features)

    results = {
        'duration': pred,
        'model': RUN_ID,
    }

    return jsonify(results)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=9696)