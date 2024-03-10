import uvicorn
import joblib

from fastapi import FastAPI
from pandas import Timestamp
import numpy as np

app = FastAPI()
model = joblib.load("pipeline.pkl")
features = model.feature_names_in_


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.post("/predict")
def predict(
    safra_abertura: int = 201905,
    cidade: str = "SAO PAULO",
    estado: str = "SP",
    idade: int = 39,
    sexo: str = "M",
    limite_total: int = 18000,
    limite_disp: int = 18126,
    data: str = "2019-11-02 00:00:00",
    valor: float = 8.0,
    grupo_estabelecimento: str = "POSTO DE GAS",
    cidade_estabelecimento: str = "SAO PAULO",
    pais_estabelecimento: str = "BR",
):
    features = np.array(
        [
            safra_abertura,
            cidade,
            estado,
            idade,
            sexo,
            limite_total,
            limite_disp,
            Timestamp(data),
            valor,
            grupo_estabelecimento,
            cidade_estabelecimento,
            pais_estabelecimento,
        ]
    )
    features = features.reshape(1, -1)
    pred = model.predict(features).tolist()[0]
    return {"prediction": pred}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
