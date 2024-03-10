from fastapi import FastAPI
import uvicorn
import joblib

app = FastAPI()
model = joblib.load("model_pipeline/pipeline.pkl")

@app.get("/health")
def health_check():
    return {"status": "ok"}

    

@app.post("/predict")
def predict(features: dict):
    # Process the features and make predictions using the loaded model
    # ...
    # Return the predictions
    model.predict(features)
    return {"predictions": predictions}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)