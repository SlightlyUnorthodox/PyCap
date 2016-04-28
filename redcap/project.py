#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Scott Burns <scott.s.burns@vanderbilt.edu>'
__license__ = 'MIT'
__copyright__ = '2014, Vanderbilt University'

import json
import warnings

from .request import RCRequest, RedcapError, RequestException
from semantic_version import Version
import semantic_version

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

try:
    from pandas import read_csv
except ImportError:
    read_csv = None

# Dictionary of all 28 API methods used by REDCap as of Version 6.11.0
#   Keys: function (create/import/export/delete) + target (arm, event, report, etc.)
#   Values: method name + earliest version number
# If the version number when introduced is not known, default to Version('6.0.0')
api_methods = {
    ('export','arm'):('export_arms',Version('6.0.0')),
    ('export','event'):('export_events',Version('6.0.0')),
    ('export','exportFieldNames'):('export_list_of_export_field_names',Version('6.0.0')),
    ('export','file'):('export_file',Version('6.0.0')),
    ('export','instrument'):('export_instruments',Version('6.0.0')),
    ('export','pdf'):('export_pdf_file_of_instruments',Version('6.4.0')),
    ('export','formEventMapping'):('export_instrument_event_mappings',Version('6.11.0')),
    ('export','metadata'):('export_metadata',Version('6.0.0')),
    ('export','project'):('export_project_info',Version('6.5.0')),
    ('export','record'):('export_records',Version('6.0.0')),
    ('export','report'):('export_reports',Version('6.0.0')),
    ('export','version'):('export_redcap_version',Version('6.0.0')),
    ('export','surveyLink'):('export_survey_link',Version('6.4.0')),
    ('export','surveyParticipantList'):('export_survey_participants',Version('6.4.0')),
    ('export','surveyQueueLink'):('export_survey_queue_link',Version('6.4.0')),
    ('export','surveyReturnCode'):('export_survey_return_code',Version('6.4.0')),
    ('export','user'):('export_users',Version('6.0.0')),
    ('import','arm'):('import_arms',Version('6.11.0')),
    ('import','event'):('import_events',Version('6.11.0')),
    ('import','file'):('import_file',Version('6.11.0')),
    ('import','formEventMapping'):('import_instrument_event_mappings',Version('6.11.0')),
    ('import','metadata'):('import_metadata',Version('6.11.0')),
    ('import','record'):('import_records',Version('6.0.0')),
    ('import','user'):('import_users',Version('6.11.0')),
    ('delete','arm'):('delete_arms',Version('6.11.0')),
    ('delete','event'):('delete_events',Version('6.11.0')),
    ('delete','file'):('delete_file',Version('6.0.0')),
    ('create','project'):('create_project',Version('6.11.0'))
}

class Project(object):
    """Main class for interacting with REDCap projects"""

    #
    # Project initialization and creation methods
    #

    # Create a new project using 64-character supertoken
    @classmethod
    def create_project(cls, url, token, project_title, purpose, purpose_other=None,project_notes=None,is_longitudinal=None,surveys_enabled=None,record_autonumbering_enabled=None,**constructor_kwargs):
        pl = {'token':token,
                'content':'project',
                'format':'json',
                'type':'flat',
                'project_title':project_title,
                'purpose':purpose,
                'purpose_other':purpose_other,
                'project_notes':project_notes,
                'is_longitudinal':is_longitudinal,
                'surveys_enabled':surveys_enabled,
                'record_autonumbering_enabled':record_autonumbering_enabled}

        rcr = RCRequest(url, pl, 'create_project')
        response = rcr.execute()
        if "error" in response[0]:
            print(response)
        else: 
            return cls(url, response, **constructor_kwargs)
        
    def __init__(self, url, token, name='', verify_ssl=True, lazy=False):
        """
        Parameters
        ----------
        url : str
            API URL to your REDCap server
        token : str
            API token to your project
        name : str, optional
            name for project
        verify_ssl : boolean, str
            Verify SSL, default True. Can pass path to CA_BUNDLE.
        """

        # API-specific variables
        self.token = token
        self.name = name
        self.url = url
        self.verify = verify_ssl
        self.redcap_version = None
        
        # Project variables
        self.metadata = None
        self.field_names = None
        # We'll use the first field as the default id for each row
        self.def_field = None
        self.field_labels = None
        self.forms = None
        self.events = None
        self.arm_nums = None
        self.arm_names = None
        self.configured = False
        self.is_longitudinal = None
        # Add more detailed project info
        #   Stored in dictionary to safe space and clarity
        self.project_info = None

        if not lazy:
            self.configure()

    def configure(self):
        try:
            self.metadata = self.export_data('metadata')
        except RequestException:
            raise RedcapError("Exporting metadata failed. Check your URL and token.")
        try:
            self.redcap_version = self.rcv()
        except:
            raise RedcapError("Determination of REDCap version failed")
        try:
            self.project_info = self.export_data('project')
            self.is_longitudinal = self.project_info['is_longitudinal']
        except RequestException:
            raise RedcapError("Exporting project information failed")
        
        self.field_names = self.filter_metadata('field_name')
        # we'll use the first field as the default id for each row
        self.def_field = self.field_names[0]
        self.field_labels = self.filter_metadata('field_label')
        self.forms = tuple(set(c['form_name'] for c in self.metadata))
        # determine whether longitudinal
        #ev_data = self._call_api(self.__basepl('event'), 'exp_event')[0]
        ev_data = self.export_data('event')
        #arm_data = self._call_api(self.__basepl('arm'), 'exp_arm')[0]
        arm_data = self.export_data('arm')

        if isinstance(ev_data, dict) and ('error' in ev_data.keys()):
            events = tuple([])
        else:
            events = ev_data
            
        if isinstance(arm_data, dict) and ('error' in arm_data.keys()):
            arm_nums = tuple([])
            arm_names = tuple([])
        else:
            arm_nums = tuple([a['arm_num'] for a in arm_data])
            arm_names = tuple([a['name'] for a in arm_data])
        self.events = events
        self.arm_nums = arm_nums
        self.arm_names = arm_names
        self.configured = True

    #
    # Private methods
    #

    # Configure base parameter list
    def __basepl(self, content, rec_type='flat', format='json'):
        """Return a dictionary which can be used as is or added to for
        payloads"""
        d = {'token': self.token, 'content': content, 'format': format}
        if content not in ['metadata', 'file']:
            d['type'] = rec_type
        return d

    def __meta_metadata(self, field, key):
        """Return the value for key for the field in the metadata"""
        mf = ''
        try:
            mf = str([f[key] for f in self.metadata
                     if f['field_name'] == field][0])
        except IndexError:
            print("%s not in metadata field:%s" % (key, field))
            return mf
        else:
            return mf

    def _call_api(self, payload, typpe, **kwargs):
        request_kwargs = self._kwargs()
        request_kwargs.update(kwargs)
        rcr = RCRequest(self.url, payload, typpe)
        return rcr.execute(**request_kwargs)

    def _check_file_field(self, field):
        """Check that field exists and is a file field"""
        is_field = field in self.field_names
        is_file = self.__meta_metadata(field, 'field_type') == 'file'
        if not (is_field and is_file):
            msg = "'%s' is not a field or not a 'file' field" % field
            raise ValueError(msg)
        else:
            return True

    def _kwargs(self):
        """Private method to build a dict for sending to RCRequest

        Other default kwargs to the http library should go here"""
        return {'verify': self.verify}

    def _check_version(self, content, action='export'):
        # Compare minimum version for function against current version
        curr_version = self.redcap_version
        try:
            method_name, method_version = api_methods[action,content]
        except KeyError:
            raise RedcapError('Method for [ ' + action + ' , ' + content + ' ] does not exist')
    
        if curr_version < method_version:
            raise RedcapError(str('Invalid REDCap version.\nCurrent REDCap version: ' + str(curr_version) + '\nEarliest REDCap version for ' + method_name + ':\t' + str(method_version)))
    #   
    # REDCap data-handling methods (Import/Export/Delete)
    #

    def export_data(self, content, action = 'export', format='json', report_id = None,arms=None,field=None, raw_or_label='raw', raw_or_label_headers='raw', exportcheckboxlabel=False, record=None, event=None, forms=None, instrument=None, all_records=None, records=None, fields=None, typpe = 'flat', events=None, event_name='label', export_survey_fields=False, export_data_access_groups = None, export_checkbox_labels = False, df_kwargs=None):

        """
        Export Data from REDCap Project

        Parameters
        ----------

        content:
            * arm - Export Arms (longitudinal only)
                * required parameters:
                    * format: ('json'), csv, xml
                * optional parameters:
                    * arms: an array of arm numbers that you wish to pull events for
                    * returnFormat: ('json'), csv, xml
                * returns:
                    * arms for the project in the format specificied
            * event - Export events for a project (longitudinal only)
                * required parameters:
                    * format: ('json'), csv, xml
                * optional parameters:
                    * arms: an array of arm numbers that you wish to pull events for
                    * returnFormat: ('json'), csv, xml
                * returns:
                    * events for the project in the format specificied
            * exportFieldNames - Exports lists of field names
                * required parameters:
                    * format: ('json'), csv, xml
                * optional parameters:
                    * field: a field's variable name, specifies field's field names to export.
                    * returnFormat: ('json'), csv, xml
                * returns:
                    * list of export/import specific field names for all fields (if field not specified)
            * file - Export file
                * required parameters:
                    * action - "export"
                    * record - the record ID
                    * field - the name of the field that contains the file
                    * event - the unique event name (longitudinal only)
                * optional parameters:
                    * returnFormat: ('json'), csv, xml
                * returns
                    * the contents of the file
            * instrument - Export Instruments
                * required parameters:
                    format: ('json'), csv, xml
                * returns
                    * instruments for the project in the format specified
            * pdf - Export PDF file of data collection instruments
                * optional parameters:
                    * record - the record ID. If blank - default, will return blank PDF
                    * event - unique event name (longitudinal only)
                        * if record not blank, event blank: will return data for all record events
                        * if record not blank, event not blank: will return data only for specific event from record
                    * allRecords - value does not matter, if passed whatsoever, exports all instruments (and events)
                    * returnFormat: ('json'), csv, xml
                * returns
                    * a PDF file containing one or all data collection instruments from the project
            * formEventMapping - Export Instrument-Event Mappings
                * required parameters:
                    * format - ('json'), csv, xml
                * optional parameters:
                    * arms - an array of arm numbers that you wish to pull eventsfor
                    * returnFormat: ('json'), csv, xml
                * returns
                    * instrument-event mappings for the project in the format specified
            * metadata - Export Metadata
                * required parameters:
                    * format: ('json'), csv, xml
                * optional parameters:
                    * fields: an array of filed names specifying specific fields you wish to pull
                    * forms: an array of form names specifying specific data collection instruments
                    * returnFormat: ('json'), csv, xml
                * returns
                    * metadata from the project
            * project - Export Project Info
                * required parameters:
                    * format: ('json'), csv , xml
                * optional parameters:
                    * returnFormat: ('json'), csv, xml
                * returns:
                    * attributes for the project in specified format
            * record - Export Records
                * required parameters:
                    * format - ('json'), csv, xml
                    * typpe:    
                        * ('flat') - output as one record per row
                        * eav - output as one data point per row
                        * Non-longitudina - will have the fields -record, field-name, value
                        * Longitudinal - will have the fields - record, field_name, value, redcap_event_name
                * optional parameters:
                    * records - an array of record names specifying records you wish to pull
                    * fields - an array of field names specifying specific fields you wish to pull
                    * forms - an array of form names you wish to pull records for
                    * events - an array of unique event names that you wish to pull records for (longitudinal only)
                    * rawOrLabel - ('raw'), label - export the raw coded values or labels for the options of multiple choice fields
                    * rawOrLabelHeaders - ('raw'), label - for the CSV headers, export the variable/field names (raw) or the field labels (label)
                    * exportCheckBoxLabel - ('false'), true - specifies the format of the checkbox field values (read more in REDCap API documentation)
                    * returnFormat - ('json'), csv, xml
                    * exportSurveyFields - ('false'), true
                    * exportDataAccessGroups - ('false'), true
                    * filterLogic - string of logic text forfilter the data to be returned by this API method
                * returns
                    * data from the project in the format and type specified orderered by record id and then by event id
            * report - Export Reports
                * required parameters:
                    * report_id - the report ID number
                    * format - ('json'), csv, xml
                * optional parameters:
                    * returnFormat - ('json'), csv, xml
                    * rawOrLabel - ('raw'), label - export the raw coded values or labels for the options of multiple choice fields
                    * rawOrLabelHeaders - ('raw'), label - for the CSV headers, export the variable/field names (raw) or the field labels (label)
                    * exportCheckBoxLabel - ('false'), true - specifies the format of the checkbox field values (read more in REDCap API documentation)
                * returns
                    * data from the project in the format and type specified, orered by the record id and then by event id
            * version - Export Redcap Versoin
                * required parameters:
                    * format - ('json'), csv, xml
                * returns
                    * the current REDCap version number as plaintext
            * surveyLink - Export a Survey Link for a Participant
                * required parameters:
                    * record - the record ID
                    * instrument - the unique instrument name
                    * event - the unique event name (longitudinal only)
                * optional parameters:
                    * returnFormat - ('json'), csv, xml
                * returns:
                    * a unique survey link (URL) in plain text format for the specified record and instrument (and event)
            * participantList - Export a Survey Participant List
                * required parameters:
                    * instrument - the unique instrument name
                    * event the unique event name (longitudinal only)
                    * format - ('json'), csv, xml
                * optional parameters:
                    * returnFormat: ('json'), csv, xml
                * returns
                    * a list of all participants for the specified survey instrument (and event)
            * surveyQueueLink - Export a survey queue link for a participant
                * required parameters:
                    * record - the record ID
                * optional parameters:
                    * returnFormat: ('json'), csv, xml
                * returns
                    * a unique survey queue link (a URL) in plaint text format
            * surveyReturnCode - Export a Survey Return Code for a Participant
                * required parameters:
                    * record - the record id
                    * instrument - the unique instrument name
                    * event - the unique event name (longitudinal only)
                * optional parameters:
                    * returnFormat: ('json'), csv, xml
                * returns
                    * a unique return code in plain text format for the specified record and instrument (and event)
            * user - Export Users
                * required parameters:
                    * format - ('json'), csv, xml
                * optional parameters:
                    * returnFormat: ('json'), csv, xml
                * returns
                    * all user-related attributes (see REDCap API documentation for full list)
        """ 

        # Require event if project is longitudinal
        if self.is_longitudinal == True and event is None and content in ('surveyReturnCode','surveyParticipantList'):
            print("Error: 'event' is required for longitudinal projects")
            return
        else:  
            event = "filler"


        # Check for dataframe format usage
        ret_format = format
        if format == 'df':
            ret_format = 'csv'

        pl = self.__basepl(content,format=ret_format)

        # Establish list of all possible parameters
        to_add = (arms,field,report_id, raw_or_label, raw_or_label_headers, exportcheckboxlabel,record, event, 
            instrument, records, fields, events, event_name, export_survey_fields, export_data_access_groups, export_checkbox_labels, forms, all_records)
        str_add = ('arms','field','report_id', 'rawOrLabel', 'rawOrLabelHeadHeaders', 'exportCheckboxLabel','record','event','instrument', 'records', 'fields', 'events', 
            'event_name', 'export_survey_fields', 'export_data_access_groups', 'export_checkbox_labels', 'form','allrecords')

        for key, data in zip(str_add, to_add):
            if data:
                #  Make a url-ok string
                if key in ('fields', 'records', 'forms', 'events'):
                    pl[key] = ','.join(data)
                else:
                    pl[key] = data

        # Process Response
        if content is "pdf":
            content, headers = self._call_api(pl, 'exp_pdf')      

            # NOTE: reusing methodology from 'export_file"'
            #REDCap adds some useful things in content-type
            if 'content-type' in headers:
                splat = [kv.strip() for kv in headers['content-type'].split(';')]
                kv = [(kv.split('=')[0], kv.split('=')[1].replace('"', '')) for kv
                      in splat if '=' in kv]
                content_map = dict(kv)
            else:
                content_map = {}
            return content, content_map
        else: 
            response, _ = self._call_api(pl, str("exp_"+content))

            # Handle dataframe output gracefully
            if format in ('json', 'csv', 'xml'):
                return response
            elif not read_csv and format == 'df':
                warnings.warn('Pandas csv_reader not available, dataframe replaced with csv format')
                return response
            elif format == 'df':
                if not df_kwargs:
                    return read_csv(StringIO(response))
                else:
                    return read_csv(StringIO(response), **df_kwargs)  

    def import_data(self, content, data = None, action="import", dateFormat = 'YMD', event= None, field = None, file = None, format='json', override='0', overwriteBehavior = 'normal', returnContent = 'count', typpe='flat', to_import = None, record = None, returnFormat='json',df_kwargs=None):
        """
        Import Data to REDCap Project

        Methods
        ----------

        content = 
            * arm - Import Arms (longitudinal and development only)
                * required parameters: 
                    * action - ('import')
                    * data - contains attributes 'arm_num' and 'name', json example:
                        [{"arm_num":"1","name":"Drug A"},
                        {"arm_num":"2","name":"Drug B"},
                        {"arm_num":"3","name":"Drug C"}]
                    * format - ('json'), csv, xml
                    * override - ('0'-false), '1'-true
                        * override = 1 is 'delete all and import'
                        * override = 0 is 'add new arms or rename existing'
                * optional parameters:
                    * returnFormat - ('json'), csv, xml, df
                * returns:
                    * number of arms imported
            * event - Import events (longitudinal and development only)
                * required parameters:
                    * action - ('delete')
                    * data - contains attributes 'arm_num' and 'name', json example:
                        [{"event_name":"Baseline","arm_num":"1","day_offset":"1","offset_min":"0",
                        "offset_max":"0","unique_event_name":"baseline_arm_1"},
                        {"event_name":"Visit 1","arm_num":"1","day_offset":"2","offset_min":"0",
                        "offset_max":"0","unique_event_name":"visit_1_arm_1"},
                        {"event_name":"Visit 2","arm_num":"1","day_offset":"3","offset_min":"0",
                        "offset_max":"0","unique_event_name":"visit_2_arm_1"}]
                    * format - ('json'), csv, xml
                    * override - ('0'-false), '1'-true
                        * override = 1 is 'delete all and import'
                        * override = 0 is 'add new events or rename existing'
                * returns:
                    * number of events imported
            * file - Upload a document attached to a record (File Upload field)
                * required parameters:
                    * action - ('import')
                    * record - the record ID
                    * field - the name of the field containing the file
                    * event - the unique event name (for longitudinal projects only)
                    * file - the contents of the file
                * optional parameters
                    * returnFormat - ('json'), csv, xml
            * formEventMapping - Import Instrument-Event Mappings (longitudinal only)
                * required parameters:
                    * format - ('json'), csv, xml
                    * data - contains 'arm_num','unique_event_name', and 'form', json example:
                        [{"arm_num":"1","unique_event_name":"baseline_arm_1","form":"demographics"},
                        {"arm_num":"1","unique_event_name":"visit_1_arm_1","form":"day_3"},
                        {"arm_num":"1","unique_event_name":"visit_1_arm_1","form":"other"},
                        {"arm_num":"1","unique_event_name":"visit_2_arm_1","form":"other"}]
                * optional parameters
                    * returnFormat - ('json'), csv, xml
                * returns:
                    * number of instrument-event mappings imported
            * metadata - Import Metadata (development only)
                * required paramaters:
                    * format - ('json'), csv, xml
                    * data - the formatted metadata to be imported, use export_data('metadata') to see format
                * optional parameters:
                    * returnFormat - ('json'), csv, xml
                * returns number of fields imported
            * record - Import Records
                * required parameters:
                    * format - ('json'), csv, xml
                    * typpe - ('flat'), eav
                        * flat - ouput as one record per row
                        * eav - input as one data per row
                    * overwriteBehavior - ('normal'), overwrite
                        * normal - blank/empty values will be ignored
                        * overwrite - blank/empty values are valid and will overwrite data
                    * data - formatted records to be imported, xml examples:
                        * eav example
                            <?xml version="1.0" encoding="UTF-8" ?>
                                <records>
                                   <item>
                                      <record></record>
                                      <field_name></field_name>
                                      <value></value>
                                      <redcap_event_name></redcap_event_name>
                                   </item>
                                </records>
                        * flat example
                            <?xml version="1.0" encoding="UTF-8" ?>
                                <records>
                                   <item>
                                      each data point as an element
                                      ...
                                   </item>
                                </records>
                * optional parameters:
                    * dateFormat - ('YMD'), MDY, DMY
                        * YMD: Y-M-D, ex/ 2016-04-28
                        * MDY: M/D/Y, ex/ 04/28/2016
                        * DMY: 28/04/2016
                    * returnContent - ('count'), ids
                        * ids: a listof all record IDs that were imported
                        * count: the number of records imported
                    * returnFormat - ('json'), csv, xml
                * returns
                    * content specified by returnContent parameter
            * user - Import Users
                * required parameters:
                    * format - ('json'), csv, xml
                    * data - user attribute data, read more in REDCap API docs, simple csv example:
                        username,design,user_rights,forms
                        harrispa,1,1,"demographics:1,day_3:1,other:1"
                        taylorr4,0,0,"demographics:1,day_3:2,other:0"
                * optional parameters:
                    * returnFormat - ('json'), csv, xml
                * returns
                    * the number of users added or updated
        """
        # Check for method existence
        self._check_version(content,action)

        # Check for required parameters
        if content is 'arm' and all([action,data,override]) is False:
            raise RedcapError('[data,override] required for import_arm')
        elif content is 'event' and all([action,data,events]) is False:
            raise RedcapError('[data,override] required for import_event')
        elif (content is 'file') and (all([action,data,file,event,override]) is False) and (self.is_longitudinal == True):
            raise RedcapError('[record,field,event,file] is required for longitudinal import_file')
        elif content is 'file' and all([action,record,field,file]) is False:
            raise RedcapError('[record,field,file] is required for import_file')
        elif content is 'formEventMapping' and all([data]) is False:
            raise RedcapError('[data] is required for import_formEventMapping')
        elif content is 'metadata' and all([data]) is False:
            raise RedcapError('[data] is required for import_metadata')    
        elif content is 'record' and all([typpe,overwriteBehavior,data]) is False:
            raise RedcapError('[typpe, overwriteBehavior, data] is required for import_record')
        elif content is 'user' and all(['data']) is False:
            raise RedcapError('[data] is required for import_user')

        # Establish list of all possible parameters
        pl = self.__basepl(content,format = returnFormat)
        to_add = (action,data,override,record,field,event,file,typpe,overwriteBehavior,dateFormat,returnContent)
        str_add = ('action','data','override','record','field','event','file','type','overwriteBehavior','dateFormat','returnContent')
        for key, data in zip(str_add, to_add):
            if data:
                #  Make a url-ok string
                if key in ('data','fields', 'records', 'forms', 'events'):
                    pl[key] = ','.join(data)
                else:
                    pl[key] = data

        if content is 'file':       
            file_kwargs = {'files': {'file': (fname, fobj)}}
            return self._call_api(pl, 'imp_file', **file_kwargs)[0]
        else:
            # Makes API call, points in direction of deletion methods
            response = self._call_api(pl, str("imp_"+content))
            
            if format == 'df':
                if not df_kwargs:
                    if self.is_longitudinal:
                        df_kwargs = {'index_col': [self.def_field, str('redcap_'+content+'_num')]}
                else:
                    df_kwargs = {'index_col': self.def_field}
                buf = StringIO(response)
                df = read_csv(buf, **df_kwargs)
                buf.close()
                return df

            if 'error' in response:
                raise RedcapError(str(response))
        
            return response
    
    def delete_data(self, content, action = 'delete', arms = None, event = None, events = None, field = None, record = None, return_format = 'json'):
        """
        Delete Data from REDCap Project

        Methods
        ----------

        content = 
            * arm - Delete Arms (longitudinal and development only)
                * required parameters: 
                    * action - ('delete')
                    * arms - an array of arm numbers that you wish to delete
                * returns:
                    * number of arms deleted
            * event - Delete events (longitudinal and development only)
                * required parameters:
                    * action - ('delete')
                    * events
                * returns:
                    * number of events deleted
            * file - Remove a document attached to a record (File Upload field)
                * required-parameters:
                    * action - ('delete')
                    * record - the record ID
                    * field - the name of the field containing the file
                    * event - the unique event name (for longitudinal projects only)
                * optional-parameters
                    * returnFormat - ('json'), csv, xml
                * returns:
                    * error message by returnFormat
        """
        # Check for method existence
        self._check_version(content,action)

        # Check for required parameters
        if content is 'arm' and all([action,arms]) is False:
            raise RedcapError('[arms] required for delete_arm')
        elif content is 'event' and all([action,events]) is False:
            raise RedcapError('[events] required for delete_event')
        elif (content is 'file') and (all([action,record,field,event]) is False) and (self.is_longitudinal == True):
            raise RedcapError('[record,field,event] is required for longitudinal delete_file')
        elif content is 'file' and all([action,record,field]) is False:
            raise RedcapError('[record,field] is required for delete_file')

        # Build full API call by parameters
        pl = self.__basepl(content,format = return_format)
        to_add = (action, arms, event, events, field, record)
        str_add = ('action', 'arms', 'event', 'events', 'fields', 'record')
        for key, data in zip(str_add, to_add):
            if data:
                pl[key] = data

        # Makes API call, points in direction of deletion methods
        response = self._call_api(pl, str("del_"+content))

        return response  

    #
    # Helper methods
    #

    def backfill_fields(self, fields, forms):
        """ Properly backfill fields to explicitly request specific
        keys. The issue is that >6.X servers *only* return requested fields
        so to improve backwards compatiblity for PyCap clients, add specific fields
        when required.

        Parameters
        ----------
            fields: list
                requested fields
            forms: list
                requested forms
        Returns:
            new fields, forms
        """
        if forms and not fields:
            new_fields = [self.def_field]
        elif fields and self.def_field not in fields:
            new_fields = list(fields)
            if self.def_field not in fields:
                new_fields.append(self.def_field)
        elif not fields:
            new_fields = self.field_names
        else:
            new_fields = list(fields)
        return new_fields

    

    def filter(self, query, output_fields=None):
        """Query the database and return subject information for those
        who match the query logic

        Parameters
        ----------
        query: Query or QueryGroup
            Query(Group) object to process
        output_fields: list
            The fields desired for matching subjects

        Returns
        -------
        A list of dictionaries whose keys contains at least the default field
        and at most each key passed in with output_fields, each dictionary
        representing a surviving row in the database.
        """
        query_keys = query.fields()
        if not set(query_keys).issubset(set(self.field_names)):
            raise ValueError("One or more query keys not in project keys")
        query_keys.append(self.def_field)
        data = self.export_records(fields=query_keys)
        matches = query.filter(data, self.def_field)
        if matches:
            # if output_fields is empty, we'll download all fields, which is
            # not desired, so we limit download to def_field
            if not output_fields:
                output_fields = [self.def_field]
            #  But if caller passed a string and not list, we need to listify
            if isinstance(output_fields, basestring):
                output_fields = [output_fields]
            return self.export_records(records=matches, fields=output_fields)
        else:
            #  If there are no matches, then sending an empty list to
            #  export_records will actually return all rows, which is not
            #  what we want
            return []

    def filter_metadata(self, key):
        """
        Return a list of values for the metadata key from each field
        of the project's metadata.

        Parameters
        ----------
        key: str
            A known key in the metadata structure

        Returns
        -------
        filtered :
            attribute list from each field
        """
        filtered = [field[key] for field in self.metadata if key in field]
        if len(filtered) == 0:
            raise KeyError("Key not found in metadata")
        return filtered

    def metadata_type(self, field_name):
        """If the given field_name is validated by REDCap, return it's type"""
        return self.__meta_metadata(field_name,
                                    'text_validation_type_or_show_slider_number')

    def names_labels(self, do_print=False):
        """Simple helper function to get all field names and labels """
        if do_print:
            for name, label in zip(self.field_names, self.field_labels):
                print('%s --> %s' % (str(name), str(label)))
        return self.field_names, self.field_labels

    # Get REDCap version
    def rcv(self):
        rc = self.export_data('version')
        if 'error' in rcv:
            warnings.warn('Version information not available for this REDCap instance')
            return ''
        if semantic_version.validate(rcv):
            return semantic_version.Version(rcv)
        else:
            return rcv

    