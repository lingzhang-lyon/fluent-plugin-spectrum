module Fluent
  class SpectrumOtherOut < Output
    # First, register the plugin. NAME is the name of this plugin
    # and identifies the plugin in the configuration file.
    Fluent::Plugin.register_output('spectrum_other', self)
    
    config_param :tag, :string, default:'alert.spectrum.out' 
    #config_param :tag, :string, :default => "alert.spectrum"
    config_param :endpoint, :string, :default => "pleasechangeme.com" #fqdn of endpoint
    config_param :interval, :integer, :default => '300' #Default 5 minutes
    config_param :user, :string, :default => "username"
    config_param :pass, :string, :default => "password"
    config_param :include_raw, :string, :default => "false" #Include original object as raw
    config_param :attributes, :string, :default => "ALL" # fields to include, ALL for... well, ALL.

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


    # This method is called before starting.
    def configure(conf)
      super 
      # Read property file and create a hash
      # @rename_rules = []
      # conf_rename_rules = conf.keys.select { |k| k =~ /^rename_rule(\d+)$/ }
      # conf_rename_rules.sort_by { |r| r.sub('rename_rule', '').to_i }.each do |r|
      #   key_regexp, new_key = parse_rename_rule conf[r]

      #   if key_regexp.nil? || new_key.nil?
      #     raise Fluent::ConfigError, "Failed to parse: #{r} #{conf[r]}"
      #   end

      #   if @rename_rules.map { |r| r[:key_regexp] }.include? /#{key_regexp}/
      #     raise Fluent::ConfigError, "Duplicated rules for key #{key_regexp}: #{@rename_rules}"
      #   end

      #   #@rename_rules << { key_regexp: /#{key_regexp}/, new_key: new_key }
      #   @rename_rules << { key_regexp: key_regexp, new_key: new_key }
      #   $log.info "Added rename key rule: #{r} #{@rename_rules.last}"
      # end

      # raise Fluent::ConfigError, "No rename rules are given" if @rename_rules.empty?

      # Read property file for varbinds and create a hash
      @varbinds = []
      conf_varbinds = conf.keys.select { |k| k =~ /^varbind(\d+)$/ }
      conf_varbinds.sort_by { |r| r.sub('varbind', '').to_i }.each do |r|
        key_varbind, key_argos = parse_rename_rule conf[r]

        if key_varbind.nil? || key_argos.nil?
          raise Fluent::ConfigError, "Failed to parse: #{r} #{conf[r]}"
        end

        if @varbinds.map { |r| r[:key_varbind] }.include? /#{key_varbind}/
          raise Fluent::ConfigError, "Duplicated rules for key #{key_varbind}: #{@varbinds}"
        end

        #@varbinds << { key_varbind: /#{key_varbind}/, key_argos: key_argos }
        @varbinds << { key_varbind: key_varbind, key_argos: key_argos }
        $log.info "Added varbinds: #{r} #{@varbinds.last}"
      end

      # Setup URL Resource
        @url = 'http://' + @endpoint.to_s + '/spectrum/restful/alarms/'
        @events_url = 'http://' + @endpoint.to_s + '/spectrum/restful/events'
    end

    def parse_rename_rule rule
      if rule.match /^([^\s]+)\s+(.+)$/
        return $~.captures
      end
    end

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
      		# # If the value on event_type is spectrum, then it means that it is already from spectrum and needs an update		
          # if (record["event"]["event_type"].has_value?("alert.processed.spectrum") && record["event"].has_key?("source_event_id") )
            # PUT alarms to update enriched fields

          # If the value on event_type is not spectrum, then it means that it is from 3rd party, needs to post new alert   
          if (!record["event"]["event_type"].has_value?("alert.processed.spectrum"))

      			# Create an empty hash
            alertNewHash=Hash.new

            # Parse thro the array hash that contains name value pairs for hash mapping and add new records to a new hash
            @varbinds.each { |varbind| 
              pp varbind[:key_varbind] varbind[:key_argos]
              alertNewHash[varbind[:key_varbind]]=record["event"][varbind[:key_argos]]
            }
            # construct the xml
            @xml =" 
            <?xml version=\"1.0\" encoding=\"UTF-8\"?>
            <rs:event-request throttlesize="10"
              xmlns:rs=\"http://www.ca.com/spectrum/restful/schema/request\"
              xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"
              xsi:schemaLocation=\"http://www.ca.com/spectrum/restful/schema/request ../../../xsd/Request.xsd\">
              <rs:event>
                <rs:target-models>
                 <rs:model mh=\"0x3d4d5d\"/>
                </rs:target-models>
             
               <!-- event ID -->
                <rs:event-type id=\"0x057f059a\"/>
             
                <!-- attributes/varbinds -->"

            alertNewHash.each do |attr, val| 
              if (val.nil? || val.empty?)
                next
              else
                @xml += "<rs:varbind id=\""+ attr + "\">"+ val +"</rs:varbind>"
              end
            end

            @xml += "</rs:event>
                  </rs:event-request>"
            $log.info "Rest url for post events" + @events_url
            $log.info "xml: " +@xml
           			
            # begin		
            #   responsePostAffEnt=RestClient::Resource.new(@urlrest,@user,@pass).put(@urlrest,:content_type => 'application/json')
            # rescue Exception => e 
            #   $log.error "Error in restful put call."
            #   log.error e.backtrace.inspect
            #   $log.error responsePostAffEnt
            # end
        
          else 
            # For now just throw to stdout
            $log.info record["event"]

          end #end of if 'event_type' is 'alert.processed.spectrum'

        end  #end of if record have 'event_type'
      end # end of loop for each record
    end  #end of emit

  end #end of class SpectrumOtherOut 
end #end of module