# Fluent::Plugin::Spectrum

fluent-plugin-spectrum is an input plug-in for [Fluentd](http://fluentd.org)

## Status
[![Build Status](https://travis-ci.org/Bigel0w/fluent-plugin-spectrum.png?branch=master)](https://travis-ci.org/Bigel0w/fluent-plugin-spectrum)
[![Gem Version](https://badge.fury.io/rb/fluent-plugin-spectrum.png)](http://badge.fury.io/rb/fluent-plugin-spectrum)
[![Test Coverage](https://codeclimate.com/github/Bigel0w/fluent-plugin-spectrum/badges/coverage.svg)](https://codeclimate.com/github/Bigel0w/fluent-plugin-spectrum)
[![Code Climate](https://codeclimate.com/github/Bigel0w/fluent-plugin-spectrum/badges/gpa.svg)](https://codeclimate.com/github/Bigel0w/fluent-plugin-spectrum)
[![Dependency Status](https://gemnasium.com/Bigel0w/fluent-plugin-spectrum.svg)](https://gemnasium.com/Bigel0w/fluent-plugin-spectrum)

## Installation

These instructions assume you already have fluentd installed. 
If you don't, please run through [quick start for fluentd] (https://github.com/fluent/fluentd#quick-start)

Now after you have fluentd installed you can follow either of the steps below:

Add this line to your application's Gemfile:

    gem 'fluent-plugin-spectrum'

Or install it yourself as:

    $ gem install fluent-plugin-spectrum

## Usage

### Input plugin

Add the following into your fluentd config.

Simple:

    <source>
      type spectrum
      endpoint spectrum.yourdomain.com 	# required, FQDN of endpoint
      username username  # required
      password password  # required
      interval 60    # optional, interval in seconds, defaults to 300
    </source>
    <match alert.spectrum>
      type stdout
    </match>

Advanced:

    <source>
      type spectrum
      endpoint spectrum.yourdomain.com 	# required, FQDN of endpoint
      username username                     # required
      password password                     # required
      interval 60                       # optional, interval in seconds, defaults to 300
      state_type file                   # optional, set the type for store state (file or redis)
      state_file /tmp/spectrum_state  # optional, file to keep state or file to get redis configure (need state_type setup)
      state_tag  spectrum             # optional, tag for store state(need state_type, state_file setup)
      tag alert.spectrum              # optional, add your own tag for tha alert
      attributes  ALARM_ID,CREATION_DATE,HOSTNAME   # optional, select attributes that you want to poll in alerts, default value is __ALL__
    </source>

    
    # using rename_key to map to new keynames
    <match alert.spectrum>
      type rename_key
      deep_rename false
      remove_tag_prefix alert.spectrum
      append_tag alert
      rename_rule1 HOSTNAME source_hostname
      rename_rule2 IP_ADDRESS source_ip
      rename_rule3 ALARM_TITLE event_name
      rename_rule4 SEVERITY criticality
      rename_rule5 CREATION_DATE creation_time
      rename_rule6 ORIGINATING_EVENT_ATTR alert_description
      rename_rule7 MODEL_STRING source_type
      rename_rule8 ALARM_ID source_event_id
      rename_rule9 GC_NAME environment
    </match>
    # using key_picker to remove extra fields
    <match alert>
      type key_picker
      keys event_type,intermediary_source,source_event_id,creation_time,criticality,event_name,source_hostname,source_ip,alert_description,source_type,environment
      add_tag_prefix processed.
    </match>
    # send to STDOUT
    <match processed.alert>
      type stdout
    </match>

Now startup fluentd

    $ sudo fluentd -c fluent.conf &

Verify:

		You should see output like the following if you have events in spectrum and connectivity works.

		FluentD Log Lines:
		2015-03-05 15:04:02 -0800 [info]: Spectrum :: Polling alerts for time period: 1425596639 - 1425596642
		2015-03-05 15:04:07 -0800 [info]: Spectrum :: returned 1 alarms for period 1425596639 - 1425596647

		Output:
		2015-03-05 15:04:00 -0800 alert.spectrum: {"event_type":"alert.spectrum","intermediary_source":"spectrumapi001.corp.yourdomain.net","ALARM_ID":"54f8e0e0-e706-12c2-0165-005056a07ac5","CREATION_DATE":"1425596640","SEVERITY":"3","ALARM_TITLE":"LOGMATCH TRAPSEND CRIT","HOSTNAME":"yourhost001.corp.yourdomain.net","IP_ADDRESS":"10.10.0.14","ORIGINATING_EVENT_ATTR":"A SEC logmatch trapsend CRIT Your Alert Message here","MODEL_STRING":"Host_Device","ACKNOWLEDGED":"false","ALARM_STATUS":"","OCCURRENCES":"1","TROUBLE_SHOOTER":"","USER_CLEARABLE":"true","TROUBLE_TICKET_ID":"","PERSISTENT":"true","GC_NAME":"Your_Global_Collection"}


### Output plugin

Add the following into your fluentd config.

```
#set source,  here use kafka as example
<source>
  type   kafka
  host   localhost  #<broker host>
  port   9092 # <broker port: default=9092>
  topics argos-parser #<listening topics(separate with comma',')>
  format json #<input text type (text|json|ltsv|msgpack)>
</source>

# once match specific tag, use spectrum output plugin
<match argos-parser>
  type spectrum
  endpoint spectrum.yourdomain.com  # required, FQDN of endpoint 
  user username # required
  pass password # required
  interval 10 # interval in seconds
  model_mh your_model_handler # required, you need to create model in spectrum first
  event_type_id your_event_type_id # required, you need to set event type in spectrum first 
  spectrum_key event_type # key in alert to check if alert is from spectrum
  spectrum_value alert.raw.spectrum # value to match is its from spectrum
  alarm_ID_key source_event_id  # key in the alert that associate with alarm_ID for calling spectrum PUT alarms api 
  
  # For 3rd party alerts
  # Create new events in Spectrum
  # Set these parameters according to varbind keys of your event type in spectrum 
  # and the keynames in your original event (which you want to push to spectrum)
  <event_rename_rules>
    #key_varbind origin_event_keyname 
    2 source_hostname
    100  creation_time 
    101  criticality
    102  source_ip 
    103  alert_description   
    104  application_name  
    105  business_unit_l2  
    106  business_unit_l3  
    107  business_unit_l4  
    108  cmdb_ci_sys_id
  </event_rename_rules>  


  # Update existing alarms from Spectrum
  # set these parameters according to keys of alarm in spectrum that you want to update
  # and the the keynames in your original event 
  <alarm_rename_rules>
    #key_spectrum_alarm origin_event_keyname 
    0xffff00f6 application_name 
    0xffff00f7 business_unit_l2
    0xffff00f8 business_unit_l3
    0xffff00f9 business_unit_l4
    0xffff00fa cmdb_ci_sysid
  </alarm_rename_rules>

</match>
```

Now startup fluentd

    $ sudo fluentd -c fluent.conf &


## To Do
* All flag to allow specifying spectrum attributes to get or get _ALL_
* Add flag to allow start date/time if users want to backfill data from a specific date. then start loop. 
* Add flag to disable loop, if users only wanted to backfill from datetime to now or specific end time. 