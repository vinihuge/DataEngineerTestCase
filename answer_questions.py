from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns


class AnswerQuestions:

    def __init__(self,
                 spark: SparkSession = None
                 ):
        if spark is None:
            spark = spark
        self.spark = spark

    def max_two_passenger_mean_distance(self):
        # Qual a distância média percorrida por viagens com no máximo 2 passageiros;
        max_two_passenger_mean_distance = self.spark.sql("""
        SELECT ROUND(AVG(trip_distance), 2) AS distancia_media_max_2_passageiros
        FROM parquet.`data/treated/data-nyc-taxi.parquet` 
        WHERE passenger_count <= 2
        """).toPandas()

        max_two_passenger_mean_distance.columns = ['']
        max_two_passenger_mean_distance = max_two_passenger_mean_distance.T.reset_index()
        max_two_passenger_mean_distance.columns = ['key', 'value']

        sns.set(rc={'figure.figsize': (15, 7)})
        ax = sns.barplot(x="key", y="value", data=max_two_passenger_mean_distance, errwidth=0)
        plt.xlabel("Viagens com no máximo 2 passageiros", fontsize=18)
        plt.ylabel("Distância média", fontsize=18)
        plt.yticks(range(0, 10, 1))
        plt.title('Distância média percorrida por viagens com no máximo 2 passageiros', fontsize=22)
        ax.bar_label(ax.containers[0])
        plt.savefig('output/1_max_two_passenger_mean_distance.png')
        plt.show()

    def three_major_vendors(self):
        # Quais os 3 maiores vendors em quantidade total de dinheiro arrecadado;
        vendor = self.spark.sql("""
            SELECT vendor_id, ROUND(SUM(total_amount), 2) as total_amount
            FROM parquet.`data/treated/data-nyc-taxi.parquet` 
            GROUP BY vendor_id
            ORDER BY total_amount DESC
            LIMIT 3
        """)
        vendor_df = vendor.toPandas()

        sns.set(rc={'figure.figsize': (15, 7)})
        ax = sns.barplot(x="vendor_id", y="total_amount", data=vendor_df, errwidth=0)
        plt.xlabel("Vendor", fontsize=18)
        plt.ylabel("Dinheiro Arrecadado", fontsize=18)
        plt.title('3 maiores vendors em quantidade total de dinheiro arrecadado', fontsize=22)
        ax.bar_label(ax.containers[0])
        plt.savefig('output/2_three_major_vendors.png')
        plt.show()

    def mensal_cash_distribution(self):
        # Faça um histograma da distribuição mensal, nos 4 anos, de corridas pagas em dinheiro;
        cash_rides = self.spark.sql("""
            WITH mensal_cash AS (
                SELECT *, MONTH(pickup_datetime) as month
                FROM parquet.`data/treated/data-nyc-taxi.parquet`
                WHERE standard_payment_type = 'Cash'
            )
            SELECT month, standard_payment_type
            FROM mensal_cash
            ORDER BY month
        """)
        cash_rides_pd = cash_rides.toPandas()

        sns.set(rc={'figure.figsize': (15, 7)})
        ax = sns.histplot(data=cash_rides_pd, x="month", binwidth=.45)
        plt.xlabel("Mês", fontsize=18)
        plt.ylabel("Ocorrências", fontsize=18)
        plt.title('Corridas pagas em Dinheiro', fontsize=22)
        plt.xticks(cash_rides_pd['month'].unique())
        plt.savefig('output/3_mensal_cash_distribution.png')
        plt.show()

    def tips_timeseries(self):
        # Faça um gráfico de série temporal contando a quantidade de gorjetas de cada dia, nos últimos 3 meses de 2011.
        tip_count = self.spark.sql("""
            WITH tips_count AS (
                SELECT *, DATE(pickup_datetime) as date
                FROM parquet.`data/treated/data-nyc-taxi.parquet`
                WHERE year = 2011
            )
            SELECT date, COUNT(*) as tip_count
            FROM tips_count
            WHERE MONTH(pickup_datetime) in (SELECT DISTINCT MONTH(pickup_datetime) as month
                                             FROM tips_count
                                             ORDER BY month DESC
                                             LIMIT 3)
            GROUP BY date
            ORDER BY date
        """)

        tip_count_pd = tip_count.toPandas()
        tip_count_pd.set_index('date', inplace=True)

        sns.set(rc={'figure.figsize': (15, 7)})
        ax = sns.lineplot(data=tip_count_pd)
        plt.xlabel("Dia", fontsize=18)
        plt.ylabel("Ocorrências", fontsize=18)
        plt.title('Quantidade de gorjetas por dia nos últimos 3 meses de 2011', fontsize=22)
        plt.xticks(tip_count_pd.index[::5], rotation=90)
        plt.legend(loc='upper right', labels=['Gorjetas'])
        plt.savefig('output/4_tips_timeseries.png')
        plt.show()

    #######################################################
    #  BONUS
    #######################################################
    def week_day_mean_time_ride(self):
        # Qual o tempo médio das corridas nos dias de semana;
        week_day_mean_time_ride_df = self.spark.sql("""
        SELECT dayofweek(pickup_datetime) as num_dayOfWeek,
               date_format(pickup_datetime, 'EEEE') as trip_dayOfWeek,
               ROUND(AVG((unix_timestamp(dropoff_datetime)-unix_timestamp(pickup_datetime))/(60)), 4) as tempo_medio_corridas
        FROM parquet.`data/treated/data-nyc-taxi.parquet` 
        GROUP BY dayofweek(pickup_datetime), date_format(pickup_datetime, 'EEEE')
        ORDER BY dayofweek(pickup_datetime)
        """).toPandas()

        week_day_mean_time_ride_df.replace({'Sunday': 'Domingo',
                                            'Monday': 'Segunda-Feira',
                                            'Tuesday': 'Terça-Feira',
                                            'Wednesday': 'Quarta-Feira',
                                            'Thursday': 'Quinta-Feira',
                                            'Friday': 'Sexta-Feira',
                                            'Saturday': 'Sabado'}, inplace=True)

        sns.set(rc={'figure.figsize': (15, 7)})
        ax = sns.barplot(x="trip_dayOfWeek", y="tempo_medio_corridas", data=week_day_mean_time_ride_df, errwidth=0)
        plt.xlabel("Dia", fontsize=18)
        plt.ylabel("Dinheiro Arrecadado", fontsize=18)
        plt.title('3 maiores vendors em quantidade total de dinheiro arrecadado', fontsize=22)
        ax.bar_label(ax.containers[0])
        plt.savefig('output/5_week_day_mean_ride_time.png')
        plt.show()

    def run(self):
        self.max_two_passenger_mean_distance()
        self.three_major_vendors()
        self.mensal_cash_distribution()
        self.tips_timeseries()
        self.week_day_mean_time_ride()
