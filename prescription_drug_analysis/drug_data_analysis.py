import pandas as pd


def load_and_clean_data():
    ''' Returns the cleaned scripts, practices and chem data sets'''
    
    scripts = pd.read_csv('~/datacourse/data-wrangling/miniprojects/dw-data/201701scripts_sample.csv.gz')
    
    col_names = ['code', 'name', 'addr_1', 'addr_2', 'borough', 'village', 'post_code']
    practices = pd.read_csv('~/datacourse/data-wrangling/miniprojects/dw-data/practices.csv.gz', names = col_names)

    # Need to drop duplicate CHEM SUB rows
    chem = pd.read_csv('~/datacourse/data-wrangling/miniprojects/dw-data/chem.csv.gz')
    chem = chem.sort_values('CHEM SUB').drop_duplicates(subset = 'CHEM SUB', keep='first')
    
    return scripts, practices, chem


def flag_opioids(chem):
    '''Add column to dataframe flagging prescription if it is an opioid'''
    
    cheme= chem.copy()
    
    opioids = ['morphine', 
           'oxycodone', 
           'methadone',
           'fentanyl', 
           'pethidine',
           'buprenorphine', 
           'propoxyphene',
           'codeine']
    
    chem['is_opioids']= chem['NAME'].str.lower().str.contains(r'|'.join(opioids))
    
    return chem  
    
    
def calculate_z_score(scripts, chem):
    '''Returns a Series of Z-scores of each practice'''
    
    scripts_with_chem = (scripts
                         .merge(chem[['CHEM SUB', 'is_opioids']],
                                  left_on = 'bnf_code', 
                                  right_on = 'CHEM SUB', 
                                  how = 'left')
                         .fillna(False))
    
    # Calculate z-score for each practice
    opioids_per_practice = scripts_with_chem.groupby('practice')['is_opioids'].mean()
    relative_opioids_per_practice = opioids_per_practice-scripts_with_chem['is_opioids'].mean()
    std_eror_per_practice = scripts_with_chem['is_opioids'].std()/(scripts_with_chem['practice'].value_counts())**.5
    opioid_scores = relative_opioids_per_practice/std_eror_per_practice
    
    return opioid_scores


def dump_data(results):
    '''Dumps pandas dataframe of the results to disk'''
    
    results.to_csv('practices_flagged.csv', index = False)

def flag_anomalous_practices(practices, scripts, opioid_scores, z_score_cutoff = 2, raw_count_cutoff = 50):
    '''Returns practices that have z-score and raw count greater than cutoff'''
    
    unique_practices = practices.sort_values('name').drop_duplicates(subset = 'code', keep = 'first')
    unique_practices = unique_practices.set_index('code')
    unique_practices['z_scores'] = opioid_scores
    unique_practices['count']= scripts['practice'].value_counts()
    results = unique_practices.sort_values('z_scores', ascending = False).head(100)                                                      
    return results.query('z_scores > @z_score_cutoff and count > @raw_count_cutoff')


if __name__ == '__main__':
    #import sys    

    #print(f"Running {sys.argv[0]}")
    #z_score_cutoff = int(sys.argv[1])
    #raw_count_cutoff = int(sys.argv[2])
    
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--z_score_cutoff',
                        default = 3,
                        type = int, 
                        help = 'The Z- score cutoff for flagging practices')
    parser.add_argument('--raw_count_cutoff', 
                        default = 50, 
                        type = int,
                        help = 'The raw count cutoff for flagging practices')
    
    args = parser.parse_args()
    print(args)
    
    scripts, practices, chem = load_and_clean_data()
    chem = flag_opioids(chem)
    opioid_scores = calculate_z_score(scripts, chem)
    anomalous_practices = flag_anomalous_practices(practices,
                                                   scripts, 
                                                   opioid_scores, 
                                                   z_score_cutoff = args.z_score_cutoff, 
                                                   raw_count_cutoff = args.raw_count_cutoff)
    dump_data(anomalous_practices)