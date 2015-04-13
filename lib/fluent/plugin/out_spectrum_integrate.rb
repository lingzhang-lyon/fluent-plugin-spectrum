module Fluent
  class SpectrumOutIntegrate < Output
    # First, register the plugin. NAME is the name of this plugin
    # and identifies the plugin in the configuration file.
    Fluent::Plugin.register_output('spectrum_integrate', self)
    
    config_param :tag, :string, default:'alert.spectrum.out' 
    #config_param :tag, :string, :default => "alert.spectrum"
    config_param :endpoint, :string, :default => "pleasechangeme.com" #fqdn of endpoint
    config_param :interval, :integer, :default => '300' #Default 5 minutes
    config_param :user, :string, :default => "username"
    config_param :pass, :string, :default => "password"
    config_param :include_raw, :string, :default => "false" #Include original object as raw
    config_param :attributes, :string, :default => "ALL" # fields to include, ALL for... well, ALL.
    config_param :model_mh, :string, :default => "model_mh"
    config_param :event_type_id, :string, :default => "event_type_id" 
    config_param :debug_post_event, :bool, :default => "false" 
    config_param :debug_put_alarm, :bool, :default => "false" 

    def parseAttributes(alarmAttribute)
      key = @spectrum_access_code[alarmAttribute['@id'].to_s].to_s
      value = ((to_utf8(alarmAttribute['$'].to_s)).strip).gsub(/\r?\n/, " ")
      return key,value
    end

    def initialize
      require 'rest-client'
      require 'json'
      require 'pp'
      require 'cgi'
      super
    end # def initialize


    def parse_rename_rule rule
      if rule.match /^([^\s]+)\s+(.+)$/
        return $~.captures
      end
    end

    ## TODO abstract this method----not work yet!!!!
    # def read_rename_mapping_to_hash(searchkeyword, array, key_searchkeyword, key_new)
    #   conf_searchkeys = conf.keys.select { |k| k =~ /^searchkeyword(\d+)$/ }
    #   conf_searchkeys.sort_by { |r| r.sub('searchkeyword', '').to_i }.each do |r|
    #     key_searchkeyword, key_new = parse_searchkeys conf[r]

    #     if key_searchkeyword.nil? || key_new.nil?
    #       raise Fluent::ConfigError, "Failed to parse: #{r} #{conf[r]}"
    #     end

    #     if @searchkeys.map { |r| r[:key_searchkeyword] }.include? /#{key_searchkeyword}/
    #       raise Fluent::ConfigError, "Duplicated rules for key #{key_searchkeyword}: #{@searchkeys}"
    #     end

    #     @searchkeys << { key_searchkeyword: key_searchkeyword, key_new: key_new }
    #     $log.info "Added searchkeys: #{r} #{@searchkeys.last}"
    #   end
    # end

    # This method is called before starting.
    def configure(conf)
      super 
      # Read property file for varbinds and create a hash
      @varbinds = []
      # TO DO-- use a commom method
      #read_rename_mapping_to_hash(varind, @varbinds, key_varbind, key_source)
      conf_varbinds = conf.keys.select { |k| k =~ /^varbind(\d+)$/ }
      conf_varbinds.sort_by { |r| r.sub('varbind', '').to_i }.each do |r|
        key_varbind, key_source = parse_rename_rule conf[r]

        if key_varbind.nil? || key_source.nil?
          raise Fluent::ConfigError, "Failed to parse: #{r} #{conf[r]}"
        end

        if @varbinds.map { |r| r[:key_varbind] }.include? /#{key_varbind}/
          raise Fluent::ConfigError, "Duplicated rules for key #{key_varbind}: #{@varbinds}"
        end

        @varbinds << { key_varbind: key_varbind, key_source: key_source }
        $log.info "Added varbinds: #{r} #{@varbinds.last}"
      end

      # Read property file for rename_rules and create a hash
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
      def resource
        RestClient::Resource.new(@events_url, :user => @user, :password => @pass, :open_timeout => 5, :timeout => (@interval * 3))
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
        ## Check if the incoming event already has an event id (alarm id) and a corresponding tag of spectrum 
        if record["event"].has_key?("event_type")

          ######3rd party alert, need 2 steps #######################   
          if (record["event"]["event_type"] != "alert.processed.spectrum")
            $log.info "The alert is from 3rd party"

            ######3rd party alert Step 1 ########################
            ######Post an event and then trigger an alarm 

      			# Create an empty hash
            alertNewHash=Hash.new

            # Parse thro the array hash that contains name value pairs for hash mapping and add new records to a new hash
            @varbinds.each { |varbind| 
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
            if debug_post_event
              $log.info "xml: " +@post_event_xml
              $log.info "debug mode, not PUT alarm yet"	
              @triggered_event_id = 'test12345'
              $log.info @triggered_event_id
            else
              $log.info "xml: " +@post_event_xml	
              begin		
                # eventPostRes = RestClient::Resource.new(@events_url,@user,@pass).post(@post_event_xml,:content_type => 'application/xml')
                eventPostRes = resource.post @post_event_xml,:content_type => 'application/xml',:accept => 'application/json'
                $log.info eventPostRes
                eventPostResBody = JSON.parse(eventPostRes.body)
                @triggered_event_id = eventPostResBody['ns1.event-response-list']['ns1.event-response']['@id']
                $log.info @triggered_event_id
              end
            end


            ######3rd party alert Step 2 ########################
            ######PUT alarm to update enriched fields
            # Create an empty hash
            alertUpdateHash=Hash.new

            # Parse thro the array hash that contains name value pairs for hash mapping and add new records to a new hash
            @rename_rules.each { |rule| 
              pp rule[:new_key]
              alertUpdateHash[rule[:key_regexp]]=record["event"][rule[:new_key]]
            }

            # construct the alarms PUT uri for update triggerd alarm withe enriched fields
            # @alarms_urlrest = @alarms_url + record["event"]["source_event_id"]
            @alarms_urlrest = @alarms_url + @triggered_event_id
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
            
            if debug_put_alarm 
              $log.info "debug mode, not PUT alarm yet"
            else
              begin 
                # alarmPutRes = resource.put @alarms_urlrest,:content_type => 'application/json'
                alarmPutRes = RestClient::Resource.new(@alarms_urlrest,@user,@pass).put(@alarms_urlrest,:content_type => 'application/json')
                $log.info alarmPutRes 
              end
            end


          ######native spectrum alert ########################
          ######PUT alarm to update enriched fields
          else 
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
            
            if debug_put_alarm 
              $log.info "debug mode, not PUT alarm yet"
            else
              begin 
                # alarmPutRes = resource.put @alarms_urlrest,:content_type => 'application/json'
                alarmPutRes = RestClient::Resource.new(@alarms_urlrest,@user,@pass).put(@alarms_urlrest,:content_type => 'application/json')
                $log.info alarmPutRes 
              end
            end




          end #end of if 'event_type' is 'alert.processed.spectrum'

        end  #end of if record have 'event_type'
      } # end of loop for each record
    end  #end of emit

  end #end of class SpectrumOtherOut 
end #end of module