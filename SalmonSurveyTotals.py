import sqlite3, pandas as pd, IPython.core.display as ip, requests
from IPython.display import display, Markdown
from datetime import datetime

connection = sqlite3.connect(":memory:")
cursor = connection.cursor()
uri = 'https://kf.kobotoolbox.org/api/v2/assets/ae8BCoHi4EmwnzP2ShmSUw/data/?format=json'

create_salmon_table_query = '''
    CREATE TABLE IF NOT EXISTS salmon (
        _id STRING PRIMARY KEY,
        Survey_Date DATE,
        Quantity INTEGER,
        Type TEXT,
        Species TEXT
    );
    '''
cursor.execute(create_salmon_table_query)
def getSurveyStats():
    dead_to_date_query = f'''
    WITH salmon_counts AS (
        SELECT
            Survey_Date,
            COALESCE(SUM(CASE WHEN Species in ('Chum', 'Coho', 'Unknown', 'Sea-run_Cutthroat') AND Type in ('Dead', 'Remnant') THEN Quantity END), 0) AS total_dead_salmon_count,
            COALESCE(SUM(CASE WHEN Species in ('Chum', 'Coho', 'Unknown', 'Sea-run_Cutthroat') AND Type = 'Live' THEN Quantity END), 0) AS total_live_salmon_count,
            COALESCE(SUM(CASE WHEN Species in ('Chum', 'Coho', 'Unknown', 'Sea-run_Cutthroat') AND Type in ('Live', 'Dead', 'Remnant') THEN Quantity END), 0) AS total_salmon_count,
            COALESCE(SUM(CASE WHEN Species = 'Chum' AND Type in ('Dead', 'Remnant') THEN Quantity END), 0) AS dead_chum_count,
            COALESCE(SUM(CASE WHEN Species = 'Chum' AND Type = 'Live' THEN Quantity END), 0) AS live_chum_count,
            COALESCE(SUM(CASE WHEN Species = 'Coho' AND Type in ('Dead', 'Remnant') THEN Quantity END), 0) AS dead_coho_count,
            COALESCE(SUM(CASE WHEN Species = 'Coho' AND Type = 'Live' THEN Quantity END), 0) AS live_coho_count,
            COALESCE(SUM(CASE WHEN Species in ('Resident_Cutthroat', 'Sea-run_Cutthroat', 'Cutthroat') AND Type = 'Dead' THEN quantity END), 0) AS dead_cutthroat_count,
            COALESCE(SUM(CASE WHEN Species in ('Resident_Cutthroat', 'Sea-run_Cutthroat', 'Cutthroat') AND Type = 'Live' THEN Quantity END), 0) as live_cutthroat_count,
            COALESCE(SUM(CASE WHEN Species = 'Unknown' AND Type = 'Dead' THEN quantity END), 0) AS dead_unknown_count,
            COALESCE(SUM(CASE WHEN Species = 'Unknown' AND Type = 'Live' THEN quantity END), 0) AS live_unknown_count,
            COALESCE(SUM(CASE WHEN Type = 'Redd' THEN Quantity END), 0) as redd_count
        FROM
            salmon
        GROUP BY
            Survey_Date
    ), running_counts AS (
        SELECT
            Survey_Date,
            SUM(dead_chum_count) OVER (ORDER BY Survey_Date) AS running_total_dead_chum,
            SUM(dead_chum_count) OVER (ORDER BY Survey_Date) + live_chum_count AS running_total_all_chum,
            SUM(dead_coho_count) OVER (ORDER BY Survey_Date) AS running_total_dead_coho,
            SUM(dead_coho_count) OVER (ORDER BY Survey_Date) + live_coho_count AS running_total_all_coho,
            SUM(dead_cutthroat_count) OVER (ORDER BY Survey_Date) AS running_total_dead_cutthroat,
            SUM(dead_cutthroat_count) OVER (ORDER BY Survey_Date) + live_cutthroat_count AS running_total_all_cutthroat,
            SUM(dead_unknown_count) OVER (ORDER BY Survey_Date) AS running_total_dead_unknown,
            SUM(dead_unknown_count) OVER (ORDER BY Survey_Date) + live_unknown_count AS running_total_all_unknown,
            SUM(total_dead_salmon_count) OVER (ORDER BY Survey_Date) AS running_total_dead_salmon,
            SUM(total_dead_salmon_count) OVER (ORDER BY Survey_Date) + total_live_salmon_count AS running_total_all_salmon
        FROM
            salmon_counts
    )
    SELECT
        sc.Survey_Date,
        sc.total_dead_salmon_count,
        sc.total_live_salmon_count,
        sc.total_salmon_count,
        sc.dead_chum_count,
        sc.live_chum_count,
        sc.dead_coho_count,
        sc.live_coho_count,
        sc.dead_cutthroat_count,
        sc.live_cutthroat_count,
        sc.redd_count,
        rc.running_total_dead_chum,
        rc.running_total_all_chum,
        rc.running_total_dead_coho,
        rc.running_total_all_coho,
        rc.running_total_dead_cutthroat,
        rc.running_total_all_cutthroat,
        rc.running_total_dead_unknown,
        rc.running_total_all_unknown,
        rc.running_total_dead_salmon,
        rc.running_total_all_salmon
    FROM
        salmon_counts sc
    JOIN running_counts rc ON sc.Survey_Date = rc.Survey_Date;
    '''
    return pd.read_sql(dead_to_date_query, connection)

salmon_insert_query = '''
        INSERT OR IGNORE INTO salmon (
        _id,
        Survey_Date,
        Quantity,
        Type,
        Species
        ) VALUES (?, ?, COALESCE(?,1), ?, ?);
    '''

def getData(uri):
    response = requests.get(uri)
    return response.json()

def processEntries(entries):
    for entry in entries:
        values = (
            entry.get('_id'),
            entry.get('Survey_Date'),
            entry.get('Quantity', 1),
            entry.get('Type'),
            entry.get('Species')
        )
        cursor.execute(salmon_insert_query, values)
        
def loadSurveyData(uri):
    allDataInserted = False
    while not allDataInserted:
        data = getData(uri)
        entries = data['results']
        processEntries(entries)
        uri = data['next']
        allDataInserted = True if uri is None else False
        
def getMaxSurveyTotal(df, species, columnName):
    max_row = df[columnName].values.argmax()
    print(f'Yearly {species} total: {df.iloc[max_row][columnName]} Calculated from survey: {df.iloc[max_row]["Survey_Date"]}')

def generateSalmonStewardsData(df):
    recent_survey_totals_query = f'''
            SELECT
                Survey_Date,
                COALESCE(SUM(CASE WHEN Species = 'Chum' AND Type = 'Live' THEN quantity END), 0) AS [Live Chum],
                COALESCE(SUM(CASE WHEN Species = 'Coho' AND Type = 'Live' THEN quantity END), 0) AS [Live Coho],
                COALESCE(SUM(CASE WHEN Species in ('Resident_Cutthroat', 'Sea-run_Cutthroat', 'Cutthroat') AND Type = 'Live' THEN quantity END), 0) AS [Live Cutthroat],
                COALESCE(SUM(CASE WHEN Species = 'Unknown' AND Type = 'Live' THEN quantity END), 0) AS [Live Unknown Salmonids],        
                COALESCE(SUM(CASE WHEN Species = 'Chum' AND Type = 'Dead' THEN quantity END), 0) AS [Dead Chum],
                COALESCE(SUM(CASE WHEN Species = 'Coho' AND Type = 'Dead' THEN quantity END), 0) AS [Dead Coho],
                COALESCE(SUM(CASE WHEN Species in ('Resident_Cutthroat', 'Sea-run_Cutthroat', 'Cutthroat') AND Type = 'Dead' THEN quantity END), 0) AS [Dead Cutthroat],
                COALESCE(SUM(CASE WHEN Species = 'Unknown' AND Type = 'Dead' THEN quantity END), 0) AS [Dead Unknown Salmonids],
                COALESCE(SUM(CASE WHEN Type = 'Redd' THEN quantity END), 0) AS [Redds]
            FROM
                salmon
            GROUP BY
                Survey_Date
            ORDER BY
                Survey_Date desc
        '''
    display(Markdown('# Salmon Survey Totals'))
    display(ip.HTML(pd.read_sql(recent_survey_totals_query, connection).fillna(0).to_html(index=False)))
    getMaxSurveyTotal(df, 'Chum', 'running_total_all_chum')
    getMaxSurveyTotal(df, 'Coho', 'running_total_all_coho')
    getMaxSurveyTotal(df, 'Cutthroat', 'running_total_all_cutthroat')
    getMaxSurveyTotal(df, 'Unknown', 'running_total_all_unknown')
    print("Note: Yearly totals are where the number of live + dead are the greatest")

##import unittest
##class AssertStatsMatchExpected(unittest.TestCase):
##    def testYearlyTotals(self):
##        actual = df.loc[df['Survey_Date'] == '2023-10-31']
##        # compare 2023-10-31 stats to expected values
##        expectedValues = ['2023-10-31', 6, 15, 21, 3, 8, 3, 1, 0, 1, 0, 5, 13, 13, 14, 0, 1, 0, 5, 18, 33]
##        for i in range(len(expectedValues)):
##            self.assertEqual(actual[actual.columns[i]].item(), expectedValues[i])
##    def runTest(self):
##        self.testYearlyTotals()

loadSurveyData(uri)
df = getSurveyStats()
##runner = unittest.TextTestRunner()
##result = runner.run(AssertStatsMatchExpected())
##if result.wasSuccessful():
generateSalmonStewardsData(df)
##else:
##    print("Internal tests have failed indicating calculations may be inaccurate. Please contact the salmon survey")
