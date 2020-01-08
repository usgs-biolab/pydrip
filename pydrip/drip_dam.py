import pandas as pd 
import numpy as np 
import sys
from shapely.geometry import Point


"""
This module combines dam information from two sources into a common dataset that manages
dam removals for the Dam Removal Information Portal.  This module has a Dam class that allows
for assigning of dam characteristics.
"""

class Dam:
    def __init__(self, dam_id, dam_source='Dam Removal Science'):
        '''
        Description
        ------------
        Initiates dam removal object
        
        Parameters
        ------------
        dam_id: str, identifier of dam
        dam_source: str, source of dam removal information (options: 'American Rivers', 'Dam Removal Science')
        '''
        self._id = str(dam_id)
        self.dam_source = str(dam_source)
        self.ar_id = None
        self.latitude = None
        self.longitude = None
        self.dam_built_year = None
        self.dam_removed_year = None
        self.dam_height_ft = None
        self.dam_name = None
        self.stream_name = None
        self.dam_alt_name = []
        self.stream_alt_name = []
        self.from_ar = []

        if dam_source == 'Dam Removal Science':
            self.in_drd = 1

        elif dam_source == 'American Rivers':
            self.in_drd = 0

        else:
            print (f'Unknown Source for id: {_id}. Only accepts "American Rivers" and "Dam Removal Science"')
            sys.exit()
    
    def science_data(self, science_data):
        '''
        Description
        ------------
        Get data about dam from dam removal science data
        
        Parameters
        ------------
        science_data: tuple of information about dam from dam removal science data
        '''
        self.science_id = str(science_data.DamAccessionNumber)

        if ~np.isnan(float(science_data.DamLatitude)) and ~np.isnan(float(science_data.DamLongitude)):
            self.latitude = float(science_data.DamLatitude)
            self.longitude = float(science_data.DamLongitude)

        #if height value in science database is not null then set value 
        if ~np.isnan(float(science_data.DamHeight_m)):
            self.dam_height_ft = float(science_data.DamHeight_m)*3.28084  #convert meters to feet

        #if removal year is not null in the science database then set value
        if ~np.isnan(science_data.DamYearRemovalFinished):
            self.dam_removed_year = int(science_data.DamYearRemovalFinished)
        
        #if year dam was built is not null in the science database then set value
        if ~np.isnan(science_data.DamYearBuiltOriginalStructure):
            self.dam_built_year = int(science_data.DamYearBuiltOriginalStructure)
        elif ~np.isnan(science_data.DamYearBuiltRemovedStructure):
            self.dam_built_year = int(science_data.DamYearBuiltRemovedStructure)

        #if dam name is not null in the science database then set value
        if str(science_data.DamName)!='nan':
            self.dam_name = str(science_data.DamName).lower()
        
        #if stream name is not null in the science database then set value
        if str(science_data.DamRiverName)!='nan':
            self.stream_name = str(science_data.DamRiverName).lower()

        #if dam alternative name is not null in the science database then set value
        if str(science_data.DamNameAlternate)!='nan':
            self.dam_alt_name = str(science_data.DamNameAlternate).lower().split(",")
        
        #if stream alternative name is not null in the science database then set value
        if str(science_data.DamRiverNameAlternate)!='nan':
            self.stream_alt_name = str(science_data.DamRiverNameAlternate).lower().split(",")

        #if american rivers id is not null in the science database then set value
        if str(science_data.AR_ID)!='nan':
            self.ar_id = str(science_data.AR_ID)

    def update_missing_data(self, american_rivers_df):
        '''
        Description
        ------------
        For a dam in dam removal science if data are missing try filling it in using american rivers database  
        
        Parameters
        ------------
        american_rivers_df: dataframe of american rivers database
        '''
        if self.ar_id is not None and len(american_rivers_df[american_rivers_df['AR_ID']==self.ar_id])>0:
            ar_id_data = american_rivers_df[american_rivers_df['AR_ID']==self.ar_id]
            ar_id_data = ar_id_data.reset_index()

            if self.latitude is None and ~np.isnan(ar_id_data['Latitude'][0]):
                self.latitude = float(ar_id_data['Latitude'][0])
                self.from_ar.append('latitude')
            if self.longitude is None and ~np.isnan(ar_id_data['Longitude'][0]):
                self.longitude = float(ar_id_data['Longitude'][0])
                self.from_ar.append('longitude')
            if self.dam_built_year is None and ~np.isnan(ar_id_data['Year_Built'][0]):
                self.dam_built_year = int(ar_id_data['Year_Built'][0])
                self.from_ar.append('dam_built_year')
            if self.dam_removed_year is None and ~np.isnan(ar_id_data['Year_Removed'][0]):
                self.dam_removed_year = int(ar_id_data['Year_Removed'][0])
                self.from_ar.append('dam_removed_year')
            if self.dam_height_ft is None and ~np.isnan(ar_id_data['Dam_Height_ft'][0]):
                self.dam_height_ft = float(ar_id_data['Dam_Height_ft'][0])
                self.from_ar.append('dam_height_ft')
            if self.stream_name is None and ar_id_data['River'][0]:
                self.stream_name = ar_id_data['River'][0]

            if ar_id_data['Dam_Name'][0]:

                #Separate alternative dam names from main dam name in American Rivers Dam Name field
                if '(' in ar_id_data['Dam_Name'][0] and ')' in ar_id_data['Dam_Name'][0]:
                    ar_dam_name, ar_alt_dam_name = clean_name(ar_id_data['Dam_Name'][0]) 
                else:
                    ar_dam_name = ar_id_data['Dam_Name'][0]
                    ar_alt_dam_name = []

                #If name is none replace with AR name
                if self.dam_name is None:
                    self.dam_name = str(ar_dam_name)
                    self.from_ar.append('dam_name')

                ar_alt_dam_name.append(ar_dam_name) #ar dam names all
                current_dam_names = self.dam_alt_name + [self.dam_name]  #all dam names currently tracked in py object
                new_names = [i for i in ar_alt_dam_name if i.lower() not in [x.lower() for x in current_dam_names] and i != '']
                if len(new_names) > 0:
                    self.dam_alt_name + list(set(new_names))
                    if len(self.dam_alt_name)>0 and len(self.dam_alt_name)>(len(current_dam_names)-1):    #For some reason it was adding dam_alt_name even if no alt name
                        self.from_ar.append('dam_alt_name')

    def ar_dam_data(self, dam_data):
        '''
        Description
        ------------
        Populate dam object properties from attributes in the American Rivers Database  
        
        Parameters
        ------------
        dam_data: tuple of information about dam including attributes from the American Rivers Database
        '''
        self.ar_id = dam_data.AR_ID
        self.latitude = dam_data.Latitude
        self.longitude = dam_data.Longitude
        self.dam_built_year = dam_data.Year_Built
        self.dam_removed_year = dam_data.Year_Removed
        self.dam_height_ft = dam_data.Dam_Height_ft
        self.stream_name = dam_data.River

        if '(' in dam_data.Dam_Name and ')' in dam_data.Dam_Name:
            ar_dam_name, ar_alt_dam_name = clean_name(dam_data.Dam_Name) 
            self.dam_name = ar_dam_name
            self.dam_alt_name = ar_alt_dam_name
        else:
            self.dam_name = dam_data.Dam_Name
    
    def add_geometry(self):
        '''
        Description
        ------------
        Convert shapely point to wkt
        '''
        if self.longitude is not None and self.latitude is not None:
            self.geometry = Point(self.longitude, self.latitude).wkt
        

def clean_name(name):
    '''
    Description
    ------------
    Removes alternative name, denoted in parentheses from a dam or river name

    Output
    -----------
    main_name: str, name with (alt_name) removed
    alt_name: list, comman separated strings representing alternative names without parentheses
    '''
    main_name = name.split('(')[0] + name.split(')')[-1]
    main_name = main_name.replace('  ', ' ')
    main_name = main_name.strip()

    extraction = name.split('(')[1]
    alt_name = extraction.split(')')[0] 
    alt_name = [alt_name.strip()]

    return main_name, alt_name
    

