import numpy as np
from dirty_cat import GapEncoder, SuperVectorizer
from feature_engine.datetime import DatetimeFeatures
from feature_engine.selection import (
    DropConstantFeatures,
    DropDuplicateFeatures,
) 
from sklearn.pipeline import make_pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import OrdinalEncoder
from sklearn.impute import SimpleImputer

import pandas as pd
import pickle
from sklearn.model_selection import (
    train_test_split,
    cross_validate,
)

# Prepare input data
df = pd.read_csv("../assets/master_table.csv", sep=";", encoding="latin1")
df.drop(columns=["id"], inplace=True)
df['data'] = pd.to_datetime(df['data'], format='%d.%m.%Y')

# @TODO hash df input, to reproduce it later on 
# @TODO - MONITORING - Check for data leakage
# @TODO - Performance - Reduce mem for variables such as int64 -> int32
# @TODO - Performance - Check feature selection to reduce dimensionality

# Split the data into train, holdout, and calibration sets
train_set, holdout_set = train_test_split(
    df,
    stratify=df['is_revenge_spending'],
    shuffle=True,
    test_size=0.20,
    random_state=42,
)

holdout_set, calibration_set = train_test_split(
    holdout_set,
    stratify=None,
    shuffle=True,
    test_size=0.10,
    random_state=42,
)

DATETIME_FEATURES = [
    "month",
    "quarter",
    "week",
    "day_of_week",
    "day_of_month",
    "weekend",
    "quarter_start",
    "quarter_end",
    "year_start",
    "year_end",
]

def get_vectorizer(
        datetime_pipeline = make_pipeline(
            DropConstantFeatures(tol=0.998, missing_values="ignore"),
            DatetimeFeatures(
                missing_values="ignore", features_to_extract=DATETIME_FEATURES
            ),
        ),
        low_card_transformer = make_pipeline(
            DropConstantFeatures(tol=0.998, missing_values="ignore"),
            SimpleImputer(
                missing_values=np.nan,
                add_indicator=True,
                strategy="constant",
                fill_value="missing",
            ),
            OrdinalEncoder(
                handle_unknown="use_encoded_value", unknown_value=-1
            ),
        ),
        high_card_transformer = make_pipeline(
            DropConstantFeatures(tol=0.998, missing_values="ignore"),
            GapEncoder(hashing=True, random_state=42),
        ),
        numerical_transformer = make_pipeline(
            DropConstantFeatures(tol=0.998, missing_values="ignore"),
            DropDuplicateFeatures(),
            SimpleImputer(
                missing_values=np.nan, add_indicator=True, strategy="median"
            ),
        )
):
    return SuperVectorizer(
            auto_cast=True,
            n_jobs=2,
            low_card_cat_transformer=low_card_transformer,
            high_card_cat_transformer=high_card_transformer,
            numerical_transformer=numerical_transformer,
            datetime_transformer=datetime_pipeline,
            impute_missing="force",
            remainder="drop",
    )

features = ['safra_abertura', 'cidade', 'estado', 'idade', 'sexo', 'limite_total',
       'limite_disp', 'data', ' valor ', 'grupo_estabelecimento',
       'cidade_estabelecimento', 'pais_estabelecimento']

X_train = train_set[features].copy()
y_train = train_set['is_revenge_spending'].copy()

X_holdout = holdout_set[features].copy()
y_holdout = holdout_set['is_revenge_spending'].copy()

X_calibration = calibration_set[features].copy()
y_calibration = calibration_set['is_revenge_spending'].copy()

vectorizer = get_vectorizer()
pipeline = make_pipeline(
    vectorizer, 
    RandomForestClassifier()
)
pipeline.fit(X_train, y_train)

# @TODO - Perform model calibration
# @TODO - MONITORING - log the model performance

# Save pipeline to a pickle file
with open('pipeline.pkl', 'wb') as file:
    pickle.dump(pipeline, file)



