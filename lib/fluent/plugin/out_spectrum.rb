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
    config_param :spectrum_key, :string, :default => "event_type" # key in the alert to check if alert is from spectrum
    config_param :spectrum_value, :string, :default => "alert.raw.spectrum"# value to match is its from spectrum

    def initialize
      require 'rest-client'
      require 'json'
      require 'pp'
      require 'cgi' # verify we need
      super
    end # def initialize
    def parse_rename_rule rule
      if rule.match /^([^\s]+)\s+(.+)$/
        return $~.captures
      end
    end
    # This method is called before starting.
    def configure(conf)
      super 
      # Read configuration for varbinds and create a hash
      @varbind_rename_rules = []
      conf_varbinds_rename_rules = conf.keys.select { |k| k =~ /^varbind(\d+)$/ }
      conf_varbinds_rename_rules.sort_by { |r| r.sub('varbind', '').to_i }.each do |r|
        key_varbind, key_source = parse_rename_rule conf[r]
        if key_varbind.nil? || key_source.nil?
          raise Fluent::ConfigError, "Failed to parse: #{r} #{conf[r]}"
        end
        if @varbind_rename_rules.map { |r| r[:key_varbind] }.include? /#{key_varbind}/
          raise Fluent::ConfigError, "Duplicated rules for key #{key_varbind}: #{@varbind_rename_rules}"
        end
        @varbind_rename_rules << { key_varbind: key_varbind, key_source: key_source }
        $log.info "Added varbind_rename_rules: #{r} #{@varbind_rename_rules.last}"
      end
      # Read configuration for rename_rules and create a hash
      @rename_rules = []
      conf_rename_rules = conf.keys.select { |k| k =~ /^rename_rule(\d+)$/ }
      conf_rename_rules.sort_by { |r| r.sub('rename_rule', '').to_i }.each do |r|
        key_regexp, new_key = parse_rename_rule conf[r]
        if key_regexp.nil? || new_key.nil?
          raise Fluent::ConfigError, "Failed to parse: #{r} #{conf[r]}"
        end
        if @rename_rules.map { |r| r[:key_regexp] }.include? /#{key_regexp}/
          raise Fluent::ConfigError, "Duplicated rules for key #{key_regexp}: #{@rename_rules}"
        end
        #@rename_rules << { key_regexp: /#{key_regexp}/, new_key: new_key }
        @rename_rules << { key_regexp: key_regexp, new_key: new_key }
        $log.info "Added rename key rule: #{r} #{@rename_rules.last}"
      end
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
        $stderr.puts "OK!"

        ######native spectrum alert ########################
        ######PUT alarm to update enriched fields
        if (record["event"].has_key?(@spectrum_key) && record["event"][@spectrum_key] == @spectrum_value) 
          
          $log.info "The alert is from spectrum"
          # Create an empty hash
          alertUpdateHash=Hash.new
          # Parse thro the array hash that contains name value pairs for hash mapping and add new records to a new hash
          @rename_rules.each { |rule| 
            pp rule[:new_key]
            alertUpdateHash[rule[:key_regexp]]=record["event"][rule[:new_key]]
          }
          # construct the alarms PUT uri for update triggerd alarm withe enriched fields
          # @alarms_urlrest = @alarms_url + record["event"]["source_event_id"]
          @alarms_urlrest = @alarms_url + record["event"]["source_event_id"]
          @attr_count = 0
          alertUpdateHash.each do |attr, val| 
            if (val.nil? || val.empty?)
              next
            else
              if (@attr_count == 0)
                @alarms_urlrest = @alarms_urlrest + "?attr=" + attr + "&val=" + CGI.escape(val.to_s)
                @attr_count +=1
              else
                @alarms_urlrest = @alarms_urlrest + "&attr=" + attr + "&val=" + CGI.escape(val.to_s)
                @attr_count +=1
              end
            end
          end
          $log.info "Rest url for PUT alarms: " + @alarms_urlrest            
          
          begin 
            # alarmPutRes = alarms_resource.put @alarms_urlrest,:content_type => 'application/json'
            alarmPutRes = RestClient::Resource.new(@alarms_urlrest,@user,@pass).put(@alarms_urlrest,:content_type => 'application/json')
            $log.info alarmPutRes 
          end

        ######3rd party alert #######################
        ######Post an event and then trigger an alarm ######   
        else
          $log.info "The alert is from 3rd party"           
          # Create an empty hash
          alertNewHash=Hash.new
          # Parse thro the array hash that contains name value pairs for hash mapping and add new records to a new hash
          @varbind_rename_rules.each { |varbind| 
            pp varbind[:key_varbind]+varbind[:key_source]
            alertNewHash[varbind[:key_varbind]]=record["event"][varbind[:key_source]]
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
          $log.info "Rest url for post events: " + @events_url               
          $log.info "xml: " +@post_event_xml    
          begin        
            # eventPostRes = RestClient::Resource.new(@events_url,@user,@pass).post(@post_event_xml,:content_type => 'application/xml')
            eventPostRes = events_resource.post @post_event_xml,:content_type => 'application/xml',:accept => 'application/json'
            $log.info eventPostRes
            eventPostResBody = JSON.parse(eventPostRes.body)
            @triggered_event_id = eventPostResBody['ns1.event-response-list']['ns1.event-response']['@id']
            $log.info "event id is: " + @triggered_event_id
          end

        end #end of if 'event_type' is 'alert.processed.spectrum' or not
      } # end of loop for each record
    end  #end of emit
  end #end of class SpectrumOtherOut 
end #end of module