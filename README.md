## DADS6005: Data Streaming and Real-Time Analytics (NIDA)
Lecturer : Asst.Prof Ekarat Rattagan 
<hr>

## This is the final assignment for building Realtime BTC price forecast in every hours (Training and Inference model for hourly forecasting) by using Airflow ecosystem, MongoDB and Docker. 
This project doing 3 main task:
- DAG 1. Get BTC price and insert to mongodb.
- DAG 2. Train Model.
- DAG 3. Forecast BTC price next 12 period (5 minutes each) in next 1 hour.

### Report
- [View Report](https://github.com/chayaphon/realtime_airflow-btcPrediction/blob/main/Report.pdf)

## License
[MIT License](https://github.com/chayaphon/realtime_airflow-btcPrediction/blob/main/LICENSE.md)

## Contact
- **Name**: Chayaphon Sornthananon
- **Email**: 6610422007@stu.nida.ac.th

## Usage
- In Ubuntu Linux before able to use "astro" command you need to execute below command in the current project directory.
"curl -sSL https://install.astronomer.io | sudo bash"

- To start project please use "astro dev start"
- To stop project please use "astro dev stop"

Docker :
 <br>  Please change ip address and port as in docker-compose.override.yml
