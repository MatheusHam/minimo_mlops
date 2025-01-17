build:
	docker build --no-cache -t test .

run:
	docker run -d --name app_container -p 8000:8000 test

mock:
	@python data_engineering/prod_data_eng_pipeline.py
	@python model_pipeline/prod_model_pipeline.py
