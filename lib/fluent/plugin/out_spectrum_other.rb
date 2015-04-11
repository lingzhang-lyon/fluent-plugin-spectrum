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
    config_param :model_mh, :string, :default => "model_mh"
    config_param :event_type_id, :string, :default => "event_type_id" 
    config_param :debug, :bool, :default => "false" 

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
      # Read property file for varbinds and create a hash
      @varbinds = []
      conf_varbinds = conf.keys.select { |k| k =~ /^varbind(\d+)$/ }
      conf_varbinds.sort_by { |r| r.sub('varbind', '').to_i }.each do |r|
        key_varbind, key_source = parse_varbinds conf[r]

        if key_varbind.nil? || key_source.nil?
          raise Fluent::ConfigError, "Failed to parse: #{r} #{conf[r]}"
        end

        if @varbinds.map { |r| r[:key_varbind] }.include? /#{key_varbind}/
          raise Fluent::ConfigError, "Duplicated rules for key #{key_varbind}: #{@varbinds}"
        end

        @varbinds << { key_varbind: key_varbind, key_source: key_source }
        $log.info "Added varbinds: #{r} #{@varbinds.last}"
      end

      # Setup URL Resource
      @events_url = 'http://' + @endpoint.to_s + '/spectrum/restful/events'
      def resource
        RestClient::Resource.new(@events_url, :user => @user, :password => @pass, :open_timeout => 5, :timeout => (@interval * 3))
      end

    end # end of def configure

    def parse_varbinds rule
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
          # If the value on event_type is not spectrum, then it means that it is from 3rd party, needs to post new alert   
          # if (!record["event"]["event_type"].has_value?("alert.processed.spectrum"))
          if (record["event"]["event_type"]!="alert.processed.spectrum")

      			# Create an empty hash
            alertNewHash=Hash.new

            # Parse thro the array hash that contains name value pairs for hash mapping and add new records to a new hash
            @varbinds.each { |varbind| 
              pp varbind[:key_varbind]+varbind[:key_source]
              alertNewHash[varbind[:key_varbind]]=record["event"][varbind[:key_source]]
            }
            # construct the xml
            @xml ="<?xml version=\"1.0\" encoding=\"UTF-8\"?>
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
                @xml += "\n <rs:varbind id=\""+ attr + "\"></rs:varbind>"
              else
                # @xml += "\n <rs:varbind id=\""+ attr + "\">"+ val +"</rs:varbind>"
                # @xml += "\n <rs:varbind id=\""+ attr + "\">"+ CGI.escape(val.to_s) +"</rs:varbind>"
                @xml += "\n <rs:varbind id=\""+ attr + "\">"+ CGI.escapeHTML(val) +"</rs:varbind>"
              end
            end

            @xml += "
                    </rs:event>
                  </rs:event-request>"

            $log.info "Rest url for post events: " + @events_url           	
            if debug	
              $log.info "xml: " +@xml
            else
              $log.info "xml: " +@xml	
              begin		
                # responsePostAffEnt = RestClient::Resource.new(@events_url,@user,@pass).post(@xml,:content_type => 'application/xml')
                responsePostAffEnt = resource.post @xml,:content_type => 'application/xml',:accept => 'application/json'
                $log.info responsePostAffEnt
              end
            end

          else # if it is spectrum native alerts
            # For now just throw to stdout
            $log.info record["event"]

          end #end of if 'event_type' is 'alert.processed.spectrum'

        end  #end of if record have 'event_type'
      } # end of loop for each record
    end  #end of emit

  end #end of class SpectrumOtherOut 
end #end of module