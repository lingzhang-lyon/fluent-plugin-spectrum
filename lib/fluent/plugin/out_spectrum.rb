module Fluent
  class SpectrumOut< Output
    # First, register the plugin. NAME is the name of this plugin
    # and identifies the plugin in the configuration file.
    Fluent::Plugin.register_output('spectrum', self)
    
    config_param :endpoint, :string, :default => "pleasechangeme.com" #fqdn of endpoint
    config_param :user, :string, :default => "username"
    config_param :pass, :string, :default => "password"
    config_param :interval, :integer, :default => '300' #Default 5 minutes
    config_param :model_mh, :string, :default => "model_mh"
    config_param :event_type_id, :string, :default => "event_type_id"

    # to differentiate alert is from spectrum or 3rd party
    config_param :spectrum_key, :string, :default => "event_type" # key in the alert to check if alert is from spectrum
    config_param :spectrum_value, :string, :default => "alert.raw.spectrum"# value to match is its from spectrum

    # for updating alert in spectrum
    config_param :alarm_ID_key, :string, :default => "source_event_id" # key in the alert that associate with alarm_ID for calling spectrum PUT alarms api 

    # to differentiate alerts is new or processed
    config_param :new_or_processed_key, :string, :default => "business_unit_l4" # key in the alert to check if alert is new
    config_param :new_alert_value, :string, :default => "alert.raw.spectrum"
    config_param :processed_alert_value, :string, :default => "alert.processed.spectrum"

    config_param :debug, :bool, :default => false

    def initialize
      require 'rest-client'
      require 'json'
      require 'cgi' # verify we need --yes, we need it, to_utf8 could not used to create valid url and xml
      super
    end # def initialize

    # function to UTF8 encode
    def to_utf8(str)
      str = str.force_encoding('UTF-8')
      return str if str.valid_encoding?
      str.encode("UTF-8", 'binary', invalid: :replace, undef: :replace, replace: '')
    end

    def parse_rename_rule rule
      if rule.match /^([^\s]+)\s+(.+)$/
        return $~.captures
      end
    end

    # This method is called before starting.
    def configure(conf)
      super
      # Read configuration for event_rename_rules and create a hash
      @event_rename_rules = []
      conf.elements.select { |element| element.name == 'event_rename_rules' }.each { |element|
        element.each_pair { |key_varbind, origin_event_keyname|
          element.has_key?(key_varbind) # to suppress unread configuration warning
          @event_rename_rules << { key_varbind: key_varbind, origin_event_keyname: origin_event_keyname }
          $log.info "Added event_rename_rules: #{@event_rename_rules.last}"
        }
      }


      # Read configuration for alarm_rename_rules and create a hash
      @alarm_rename_rules = []
      conf.elements.select { |element| element.name == 'alarm_rename_rules' }.each { |element|
        element.each_pair { |key_spectrum_alarm, origin_event_keyname|
          element.has_key?(key_spectrum_alarm) # to suppress unread configuration warning
          @alarm_rename_rules << { key_spectrum_alarm: key_spectrum_alarm, origin_event_keyname: origin_event_keyname }
          $log.info "Added alarm_rename_rules: #{@alarm_rename_rules.last}"
        }
      }

      
      # Setup URL Resource
      @alarms_url = 'http://' + @endpoint.to_s + '/spectrum/restful/alarms/'
      @events_url = 'http://' + @endpoint.to_s + '/spectrum/restful/events'
      def events_resource
        RestClient::Resource.new(@events_url, :user => @user, :password => @pass, :open_timeout => 5, :timeout => (@interval * 3))
      end

      def alarms_resource
        RestClient::Resource.new(@alarms_url, :user => @user, :password => @pass, :open_timeout => 5, :timeout => (@interval * 3))
      end


    end # end of def configure
    # This method is called when starting.
    def start
      super
    end
    # This method is called when shutting down.
    def shutdown
      super
    end
    # This method is called when an event reaches Fluentd.
    # 'es' is a Fluent::EventStream object that includes multiple events.
    # You can use 'es.each {|time,record["event"]| ... }' to retrieve events.
    # 'chain' is an object that manages transactions. Call 'chain.next' at
    # appropriate points and rollback if it raises an exception.
    #
    # NOTE! This method is called by Fluentd's main thread so you should not write slow routine here. It causes Fluentd's performance degression.
    def emit(tag, es, chain)
      chain.next
      es.each {|time,record|        
          ######native spectrum alert ########################
          if (record["event"].has_key?(@spectrum_key) && record["event"][@spectrum_key] == @spectrum_value) 
            $log.info "Spectrum Output :: The alert is from spectrum" 

            ## the alert is new, need to update                        
            if (record["event"].has_key?(@new_or_processed_key) && record["event"][@new_or_processed_key] == @new_alert_value )                                 
              $log.info "Spectrum Output :: The alert is new, need to be updated"

              # has @alarm_ID_key(like 'source_event_id') in the alerts, so it can be updated
              # PUT alarm to update enriched fields 
              if record["event"].has_key?(@alarm_ID_key) && !(record["event"][@alarm_ID_key].nil? || record["event"][@alarm_ID_key].empty?) 
                $log.info "Spectrum Output :: alarm_id \"#{record["event"][@alarm_ID_key]}\""
                # Create an empty hash
                alertUpdateHash=Hash.new
                # Parse thro the array hash that contains name value pairs for hash mapping and add new records to a new hash
                @alarm_rename_rules.each { |rule| 
                  # puts rule[:origin_event_keyname] + ":" + record["event"][rule[:origin_event_keyname]]
                  alertUpdateHash[rule[:key_spectrum_alarm]]=record["event"][rule[:origin_event_keyname]]
                }
                # construct the alarms PUT uri for update triggerd alarm withe enriched fields
                @alarms_urlrest = @alarms_url + record["event"][@alarm_ID_key]
                # @alarms_urlrest = @alarms_url + record["event"]["source_event_id"]  # argos specific code
                @attr_count = 0
                alertUpdateHash.each do |attr, val| 
                  if (val.nil? || val.empty?)
                    next
                  else
                    if (@attr_count == 0)
                      @alarms_urlrest = @alarms_urlrest + "?attr=" + attr + "&val=" + CGI.escape(val.to_s)
                      # @alarms_urlrest = @alarms_urlrest + "?attr=" + attr + "&val=" + to_utf8(val.to_s)
                      @attr_count +=1
                    else
                      @alarms_urlrest = @alarms_urlrest + "&attr=" + attr + "&val=" + CGI.escape(val.to_s)
                      # @alarms_urlrest = @alarms_urlrest + "&attr=" + attr + "&val=" + to_utf8(val.to_s)
                      @attr_count +=1
                    end
                  end
                end
                $log.info "Spectrum Output :: Rest url for PUT alarms: " + @alarms_urlrest            
                
                begin 
                  # alarmPutRes = alarms_resource.put @alarms_urlrest,:content_type => 'application/json'
                  alarmPutRes = RestClient::Resource.new(@alarms_urlrest,@user,@pass).put(@alarms_urlrest,:content_type => 'application/json')
                  $log.info "Spectrum Output :: "+ alarmPutRes 
                end

              else # don't have @alarm_ID_key,  could not be updated
                $log.error "Spectrum Output :: The alert missing #{@alarm_ID_key},  could not be updated"

              end

            # the alert is aleady processced by argos
            elsif (record["event"].has_key?(@new_or_processed_key) && record["event"][@new_or_processed_key] == @processed_alert_value )            
              $log.info "Spectrum Output :: The alert is already processed, no need to update enriched fields again"

            else
              $log.info "Spectrum Output :: The alert don't have correct business_unit_l4, could not determine it's processed or not, also ignore"

            end


          ######3rd party alert #######################
          ######Post an event and then trigger an alarm ######   
          else
            $log.info "Spectrum Output :: The alert is from 3rd party"           
            # Create an empty hash
            alertNewHash=Hash.new
            # Parse thro the array hash that contains name value pairs for hash mapping and add new records to a new hash
            @event_rename_rules.each { |rule| 
              if(debug)
                $log.info rule[:key_varbind]+": "+ rule[:origin_event_keyname]
              end
              alertNewHash[rule[:key_varbind]]=record["event"][rule[:origin_event_keyname]]
            }
            # construct the xml
            @post_event_xml ="<?xml version=\"1.0\" encoding=\"UTF-8\"?>
            <rs:event-request throttlesize=\"10\"
              xmlns:rs=\"http://www.ca.com/spectrum/restful/schema/request\"
              xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"
              xsi:schemaLocation=\"http://www.ca.com/spectrum/restful/schema/request ../../../xsd/Request.xsd\">
              <rs:event>
                <rs:target-models>
                 <rs:model mh= \"#{model_mh}\" />
                </rs:target-models>
             
               <!-- event ID -->
                <rs:event-type id=\"#{event_type_id}\"/>
             
                <!-- attributes/varbinds -->"
            alertNewHash.each do |attr, val| 
              if (val.nil? || val.empty?)
                @post_event_xml += "\n <rs:varbind id=\""+ attr + "\"></rs:varbind>"
              else
                @post_event_xml += "\n <rs:varbind id=\""+ attr + "\">"+ CGI.escapeHTML(val) +"</rs:varbind>"
              end
            end
            @post_event_xml += "
                    </rs:event>
                  </rs:event-request>"
            @triggered_event_id = ''
            if(debug)
              $log.info "Spectrum Output :: Rest url for post events: " + @events_url               
              $log.info "Spectrum Output :: xml: " +@post_event_xml 
            end   
            begin        
              # eventPostRes = RestClient::Resource.new(@events_url,@user,@pass).post(@post_event_xml,:content_type => 'application/xml')
              eventPostRes = events_resource.post @post_event_xml,:content_type => 'application/xml',:accept => 'application/json'
              $log.info "Spectrum Output :: " + eventPostRes
              eventPostResBody = JSON.parse(eventPostRes.body)
              @triggered_event_id = eventPostResBody['ns1.event-response-list']['ns1.event-response']['@id']
              # $log.info "event id is: " + @triggered_event_id
            end

          end #end of checking alerts is from 3rd party or spectrum

      } # end of loop for each record
    end  #end of emit
  end #end of class SpectrumOtherOut 
end #end of module