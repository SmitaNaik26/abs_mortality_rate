# abs_mortality_rate

creating the project and dataset to run the code
gcloud init
gcloud projects create smitanaik --name="smitanaik"
gcloud config set project smitanaik
bq mk --dataset smitanaik:abs
