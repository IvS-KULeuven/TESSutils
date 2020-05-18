#!/usr/bin/env python
import pickle, re
import pandas as pd
from pathlib import Path
import collections.abc 

def collect_corrected_lc(outputdir=Path('lc_corrected'),
                         inputdir = Path.cwd(),
                         file_pattern = 'tess*_sec*_corrected.pickled',
                         tic_regex = 'tess(\d+)_sec\d+_corrected.pickled',
                         sector_regex = 'tess\d+_sec(\d+)_corrected.pickled',
                         outputname_pattern = 'tess{TIC}_allsectors_corrected.pickled',
                         updates=[],
                         TICs='all'):

    ### Validate arguments  
  
    # Ensure file_pattern is a string instance that ends with ".pickled"
    if not isinstance(file_pattern ,str):
        raise TypeError('file_pattern must be a string instance that ends with ".pickled"')
    else:
        if (not file_pattern .endswith('.pickled')):
            raise TypeError('file_pattern must be a string instance that ends with ".pickled"')

    # Ensure tic_regex is a string instance of a regular expression that group the TIC number and ends with ".pickled'
    if not isinstance(tic_regex ,str):
        raise TypeError('tic_regex must be string instance of a regular expression that group the TIC number as "(\d+)" and ends with ".pickled". Ex: "tess(\d+)_sec\d+_corrected.pickled"')
    else:
        if (not tic_regex.endswith('.pickled')) \
        or (not '(\d+)' in tic_regex):
            raise TypeError('tic_regex must be string instance of a regular expression that group the TIC number as "(\d+)" and ends with ".pickled". Ex: "tess(\d+)_sec\d+_corrected.pickled"')
    
    # Ensure sector_regex is a string instance that ends with ".pickled"
    if not isinstance(sector_regex ,str):
        raise TypeError('sector_regex must be a string instance that ends with ".pickled"')
    else:
        if (not sector_regex .endswith('.pickled')) \
        or (not '(\d+)' in sector_regex):
            raise TypeError('sector_regex must be string instance of a regular expression that group the sector number as "(\d+)" and ends with ".pickled". Ex: "tess\d+_sec(\d+)_corrected.pickled"')
    
    # Ensure outputname_pattern is a string instance that contains {TIC} and ends with ".pickled"
    if not isinstance(outputname_pattern ,str):
        raise TypeError('outputname_pattern must be a string instance that contains {TIC} and ends with ".pickled". Ex: "tess{TIC}_allsectors_corrected.pickled"')
    else:
        if (not outputname_pattern .endswith('.pickled')) \
        or (not '{TIC}' in outputname_pattern):
            raise TypeError('outputname_pattern must be a string instance that contains {TIC} and ends with ".pickled". Ex: "tess{TIC}_allsectors_corrected.pickled"')
    if not isinstance(inputdir,Path):
        raise TypeError('inputdir must be a Path instance. Ex: outputdir=pathlib.Path.cwd()')
    
    # Ensure outputdir and inputdir are a Path instance
    if not isinstance(outputdir,Path):
        raise TypeError('outputdir must be a Path instance. Ex: outputdir=pathlib.Path.cwd()')
    
    # Create the output directory if needed
    if outputdir.exists():
        if not outputdir.is_dir():
            raise ValueError('The outputdir exist but is not a directory. It must be a directory')
    else:
        outputdir.mkdir()
        
    ### Beginning of the actual program
    
    # Get the filepaths and filenames of the files to process 
    filepaths = [file for file in inputdir.glob(file_pattern)]
    filenames = [file.name for file in filepaths]
    files = pd.DataFrame({'filename':filenames, 'filepath':filepaths})

    # Initialize columns for the TIC and sector number
    files['tic'] = -1
    files['sector'] = -1
    # Read the TIC number info from the filename and use that info to sort
    return_TIC = lambda name: re.match(tic_regex,name).group(1)
    files['tic'] = files['filename'].apply(return_TIC)
    files['tic'] = files['tic'].astype('int64')
    # Read the Sector number info from the filename and use that info to sort
    return_SECTOR = lambda name: re.match(sector_regex,name).group(1)
    files['sector'] = files['filename'].apply(return_SECTOR)
    files['sector'] = files['sector'].astype('int32')
    # Sort by TIC and sector number
    files.sort_values(by=['tic','sector'], inplace=True)

    # Select the TIC number to process
    if TICs != 'all':
        if isinstance(TICs,str):
            TIC = TICs
            files = files.query(f'tic == {TIC}')  
        if isinstance(TICs,list):
            files = pd.concat([files.query(f'tic == {TIC}') for TIC in TICs])

    # Group filenames by the TIC number
    groups = files.groupby('tic')

    # Loop over each TIC group (i.e., collect the sectors for a same TIC)
    counter = 1
    for tic,group in groups:

        # Print process
        print(f'Grouping TIC {tic}: [{counter}/{groups.ngroups}]')
        
        # List to store the summaries of all and each sectors
        results = [] 

        # Loop over each row of the group (i.e. loop over each sector)
        for row in group.iloc:
            # Unpickle
            filepath = row['filepath'].as_posix()
            with open(filepath, 'rb') as picklefile:
                try:
                    result = pickle.load(picklefile)
                except EOFError as e:
                    print('Skipped: file {filepath} seems to to empty.')
                    
            # Optionally, update result
            for update in updates:
                result = update_dic(result,update)
                    
            # Collect
            results.append(result)
            
        # Save to a new pickle file
        outputname = outputdir/Path(outputname_pattern.format(TIC=tic))
        with open(outputname.as_posix(), 'wb') as file: 
            pickle.dump(results, file)

        # Update counter
        counter += 1

def update_dic(dic, update, addkey=False): 
    '''
    Based on
    --------
    https://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth/3233356#3233356

    Parameters
    ----------
    dic : dict()
        Original dictionary to be updated.
    update : dict()
        New dictionary containing the updated values.
    add_key : bool, optional
        If update contains keys not in dic, do not add those nwe keys to dict.
        The default is False.

    Returns
    -------
    dic : dictionary
        updated dictionary.
    '''
    for k, v in update.items(): 
        if isinstance(v, collections.abc.Mapping):
            dic[k] = update_dic(dic.get(k, {}), v) 
        else:
            if not addkey:
                try:
                    if k in dic: 
                        dic[k] = v
                except TypeError:
                    pass
            else:
                dic[k] = v
    return dic 

if __name__ == '__main__':

    ### Custom run 1: __Minimal__
#     collect_corrected_lc()
    
    
    ### Custom run 2: __Very tailored__
    
    # I/O directories
    inputdir = Path('lc_corrected')
    outputdir=Path('lc_corrected/sector_grouped')
    
    # Pattern: Glob expression used to search the pickled files to be grouped
    file_pattern = 'tess*_sec*_corrected.pickled'
    # Pattern: Regular expression used to identify the TIC number from the filename
    tic_regex = 'tess(\d+)_sec\d+_corrected.pickled'
    # Pattern: Regular expression used to identify the TESS sector number from the filename
    sector_regex = 'tess\d+_sec(\d+)_corrected.pickled'
    # Pattern: Python f-string used to save the new pickle files. Characters {TIC} will automatically be replaced for the TIC number
    outputname_pattern = 'tess{TIC}_allsectors_corrected.pickled'

    # Set to None values that can cause problems when unpickling
    updates = [{'pca_used':{'rc':None}},    #Corrector object instance from LightKurve
               {'pca_used':{'dm':None}},    #Desing matrix object instance from LightKurve
               {'pca_all':{'rc':None}},     #Corrector object instance from LightKurve
               {'pca_all':{'dm':None}},     #Desing matrix object instance from LightKurve
               {'fit':{'TargetStar':None}}, #Model object instance from Astropy
               {'fit':{'Neighbours':None}}, #Model object instance from Astropy
               {'fit':{'Plane':None}}]      #Model object instance from Astropy

    # TICs to consider
    TICs = 'all'
    
    # Group the pickle files
    collect_corrected_lc(outputdir=outputdir,
                         inputdir=inputdir,
                         file_pattern=file_pattern,
                         tic_regex=tic_regex,
                         sector_regex=sector_regex,
                         outputname_pattern=outputname_pattern,
                         updates=updates,
                         TICs=TICs)
