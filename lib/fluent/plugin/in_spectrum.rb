module Fluent
  class SpectrumInput < Input
    Fluent::Plugin.register_input('spectrum', self)
    
    # Configurations
    INTERVAL_MIN = 10 # shoud stay above 10, avg response is 5-7 seconds
    config_param  :tag,           :string,  :default => "alert.spectrum"
    config_param  :endpoint,      :string,  :default => nil
    config_param  :username,      :string,  :default => nil
    config_param  :password,      :string,  :default => nil
    
    config_param  :state_tag,     :string,  :default => "spectrum"
    config_param  :state_type,    :string,  :default => "memory"
    config_param  :state_file,    :string,  :default => nil
    config_param  :redis_host,    :string,  :default => nil
    config_param  :redis_port,    :string,  :default => nil

    config_param  :attributes,    :string,  :default => "__ALL__"
    config_param  :interval,      :integer, :default => INTERVAL_MIN
    config_param  :select_limit,  :integer, :default => 10000
    config_param  :include_raw,   :bool,    :default => "false"

    # to differentiate alerts is new or processed
    # if not specified, will create a "event_type" field for this purpose and always set to "alert.raw.spectrum"
    config_param :new_or_processed_key,  :string, :default => nil # like "CUSTOM_EVENT_TYPE" # key in the alert to check if alert is new, the same key(may be renamed) will also be checked in output plugin
    config_param :new_alert_value,       :string, :default => "alert.raw.spectrum"
    config_param :processed_alert_value, :string, :default => "alert.processed.spectrum"

    

    # Classes
    class TimerWatcher < Coolio::TimerWatcher
      def initialize(interval, repeat, &callback)
        @callback = callback
        super(interval, repeat)
      end # def initialize
      
      def on_timer
        @callback.call
      rescue
        $log.error $!.to_s
        $log.error_backtrace
      end # def on_timer
    end


    # function to UTF8 encode
    def to_utf8(str)
      str = str.force_encoding('UTF-8')
      return str if str.valid_encoding?
      str.encode("UTF-8", 'binary', invalid: :replace, undef: :replace, replace: '')
    end

    def parseAttributes(alarmAttribute)
      key = @spectrum_access_code[alarmAttribute['@id'].to_s].to_s
      value = ((to_utf8(alarmAttribute['$'].to_s)).strip).gsub(/\r?\n/, " ")
      return key,value
    end

    def initialize
      require 'rest-client'
      require 'json'
      require 'highwatermark'
      require 'yaml'
      super
    end # def initialize

    def configure(conf)
      super 
      @conf = conf
      # Verify configs
      # Stop if required fields are not set
      unless @endpoint && @username && @password
        raise ConfigError, "Spectrum Input :: ConfigError 'endpoint' and 'username' and 'password' must be all specified."
      end
      # Enforce min interval
      if @interval.to_i < INTERVAL_MIN
        raise ConfigError, "Spectrum Input :: ConfigError 'interval' must be #{INTERVAL_MIN} or over."
      end
      # Warn about optional state file
      unless @state_type == "file" || @state_type =="redis"
        $log.warn "Spectrum Input :: 'state_type' is not set to file or redis"
        $log.warn "Spectrum Input :: state file or redis are recommended to save the last known good timestamp to resume event consuming"
      end

      @highwatermark_parameters={
        "state_tag" => @state_tag,     
        "state_type" => @state_type,
        "state_file" => @state_file,
        "redis_host" => @redis_host,
        "redis_port" => @redis_port      
      }
      $log.info "highwatermark_parameters: #{@highwatermark_parameters}"

      # default setting for @spectrum_access_code
      @spectrum_access_code={
        "0x11f9c" => "ALARM_ID",
        "0x11f4e" => "CREATION_DATE",
        "0x11f56" => "SEVERITY",
        "0x12b4c" => "ALARM_TITLE",
        "0x1006e" => "HOSTNAME",
        "0x12d7f" => "IP_ADDRESS",
        "0x1296e" => "ORIGINATING_EVENT_ATTR",
        "0x10000" => "MODEL_STRING",  
        "0x11f4d" => "ACKNOWLEDGED",
        "0x11f4f" => "ALARM_STATUS",
        "0x11fc5" => "OCCURRENCES",
        "0x11f57" => "TROUBLE_SHOOTER",
        "0x11f9b" => "USER_CLEARABLE",
        "0x12022" => "TROUBLE_TICKET_ID",
        "0x12942" => "PERSISTENT",
        "0x12adb" => "GC_NAME",
        "0x57f0105" => "CUSTOM_PROJECT",
        "0x11f51" => "CLEARED_BY_USER_NAME",
        "0x11f52" => "EVENT_ID_LIST",
        "0x11f53" => "MODEL_HANDLE",
        "0x11f54" => "PRIMARY_ALARM",
        "0x11fc4" => "ALARM_SOURCE",
        "0x11fc6" => "TROUBLE_SHOOTER_MH",
        "0x12a6c" => "TROUBLE_SHOOTER_EMAIL",
        "0x1290d" => "IMPACT_SEVERITY",
        "0x1290e" => "IMPACT_SCOPE",
        "0x1298a" => "IMPACT_TYPE_LIST",
        "0x12948" => "DIAGNOSIS_LOG",
        "0x129aa" => "MODEL_ID",
        "0x129ab" => "MODEL_TYPE_ID",
        "0x129af" => "CLEAR_DATE",
        "0x12a04" => "SYMPTOM_LIST_ATTR",
        "0x12a6f" => "EVENT_SYMPTOM_LIST_ATTR",
        "0x12a05" => "CAUSE_LIST_ATTR",
        "0x12a06" => "SYMPTOM_COUNT_ATTR",
        "0x12a70" => "EVENT_SYMPTOM_COUNT_ATTR",
        "0x12a07" => "CAUSE_COUNT_ATTR",
        "0x12a63" => "WEB_CONTEXT_URL",
        "0x12a6b" => "COMBINED_IMPACT_TYPE_LIST",
        "0x11f50" => "CAUSE_CODE",
        "0x10009" => "SECURITY_STRING"
        # "0xffff00f1" => "CUSTOM_APPLICATION_NAME",
        # "0xffff00f2" => "CUSTOM_BUSINESS_UNIT_I2",
        # "0xffff00f3" => "CUSTOM_BUSINESS_UNIT_I3",
        # "0xffff00f4" => "CUSTOM_BUSINESS_UNIT_I4",
        # "0xffff00f5" => "CUSTOM_CMDB_CI_SYSTEM"
      }

      # Read configuration for custom_attributes, and add to @spectrum_access_code
      @custom_attributes = []
      conf.elements.select { |element| element.name == 'custom_attributes' }.each { |element|
        element.each_pair { |custom_attribute_code, custom_attribute_name|
          element.has_key?(custom_attribute_code) # to suppress unread configuration warning
          @custom_attributes << { custom_attribute_code: custom_attribute_code, custom_attribute_name: custom_attribute_name }
          @spectrum_access_code.store(custom_attribute_code,custom_attribute_name)
          $log.info "Added custom_attributes: #{@custom_attributes.last}"
        }
      }



      # Create XML chunk for attributes we care about
      @attr_of_interest=""
      if(@attributes.upcase === "__ALL__")
        $log.info "Spectrum Input :: all attributes"
        @spectrum_access_code.each do |key, value|
          $log.info "key: #{key},  value: #{value}"
          @attr_of_interest += " <rs:requested-attribute id=\"#{key}\"/>"
        end
      else
        $log.info "Spectrum Input :: selected attributes"
        @attributes.split(",").each {|attr|         
          key=""
          value=""
          # if it's hex code
          if @spectrum_access_code.has_key?(attr.strip)
            key = attr.strip
            value = @spectrum_access_code.fetch(key)
          # if it's the name
          elsif @spectrum_access_code.has_value?(attr.strip.upcase)
            value = attr.strip.upcase
            key = @spectrum_access_code.key(value)
          # if it's invalid input, not the hex code or name in the map
          else 
            raise ConfigError, "Spectrum Input :: ConfigError attribute '#{attr}' is not in the hash map"
          end
          $log.info "key: #{key},  value: #{value}"
          @attr_of_interest += " <rs:requested-attribute id=\"#{key}\"/>"
        }

        if !(@custom_attributes.nil? || @custom_attributes.empty?)
          $log.info "Spectrum Input :: custom attributes"
          @custom_attributes.each{ |row|
            # TO DO
            key = row[:custom_attribute_code]
            value = row[:custom_attribute_name]
            $log.info "key: #{key},  value: #{value}"
            @attr_of_interest += " <rs:requested-attribute id=\"#{key}\"/>"

          }
        end

      end

      # URL Resource
      def resource
        @url = 'http://' + @endpoint.to_s + '/spectrum/restful/alarms'
        RestClient::Resource.new(@url, :user => @username, :password => @password, :open_timeout => 5, :timeout => (@interval * 3))
      end
    end # def configure

    def start
      @stop_flag = false
      @highwatermark = Highwatermark::HighWaterMark.new(@highwatermark_parameters)
      @loop = Coolio::Loop.new
      @loop.attach(TimerWatcher.new(@interval, true, &method(:on_timer)))
      @thread = Thread.new(&method(:run))
    end # def start

    def shutdown
      #@loop.watchers.each {|w| w.detach}
      @stop_flag = true
      @loop.stop
      @thread.join
    end # def shutdown

    def run
      @loop.run
    rescue
      $log.error "unexpected error", :error=>$!.to_s
      $log.error_backtrace
    end # def run

    def parse_and_emit_the_alarm(alarm, pollingEnd)
        # Create initial structure
        record_hash = Hash.new # temp hash to hold attributes of alarm
        raw_array = Array.new # temp hash to hold attributes of alarm for raw
        #record_hash['event_type'] = @tag.to_s
        record_hash['intermediary_source'] = @endpoint.to_s
        record_hash['receive_time_input'] = pollingEnd.to_s
        # iterate though alarm attributes
        alarm['ns1.attribute'].each do |attribute|
          key,value = parseAttributes(attribute)
          record_hash[key] = value
          if @include_raw.to_s == "true"
            raw_array << { "#{key}" => "#{value}" }
          end
        end
        # append raw object
        if @include_raw.to_s == "true"  
          record_hash[:raw] = raw_array
        end

        # log the alarm id for each alert
        if record_hash['ALARM_ID'] 
          $log.info "Spectrum Input :: alarm_id \"#{record_hash['ALARM_ID']}\""
        end 

        ####### argos specific code: 
        # if @new_or_processed_key itself not specified, always set event_type to "alert.raw.spectrum"
        if @new_or_processed_key.nil? 
          record_hash['event_type'] = "alert.raw.spectrum"
        else
          # if the value for @new_or_processed_key is not null, means it's already processed by argos
          if (record_hash[@new_or_processed_key].nil? || record_hash[@new_or_processed_key].empty?) # the alert is new, not processed by argos yet
            record_hash[@new_or_processed_key] = @new_alert_value
          else
            record_hash[@new_or_processed_key] = @processed_alert_value
          end
        end
        ####### end of argos specific code

        Engine.emit(@tag, record_hash['CREATION_DATE'].to_i,record_hash)      
    end


    def on_timer
      if not @stop_flag
        pollingStart = Engine.now.to_i
        if @highwatermark.last_records(@state_tag)
          alertStartTime = @highwatermark.last_records(@state_tag)
          $log.info "Spectrum Input :: got hwm from state file: #{alertStartTime.to_i}"
        else
          alertStartTime = (pollingStart.to_i - @interval.to_i)
          $log.info "Spectrum Input :: no hwm, got new alert start time: #{alertStartTime.to_i}"
        end
        alertEndTime = Engine.now.to_i
        pollingEnd = ''
        pollingDuration = ''
        # Format XML for spectrum post
        @xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
        <rs:alarm-request throttlesize=\"#{select_limit}\"
        xmlns:rs=\"http://www.ca.com/spectrum/restful/schema/request\"
        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"
        xsi:schemaLocation=\"http://www.ca.com/spectrum/restful/schema/request ../../../xsd/Request.xsd \">
        <rs:attribute-filter>
          <search-criteria xmlns=\"http://www.ca.com/spectrum/restful/schema/filter\">
          <filtered-models>
            <and>
              <greater-than-or-equals>
                <attribute id=\"0x11f4e\">
                  <value> #{alertStartTime} </value>
                </attribute>
              </greater-than-or-equals>
              <less-than>
                <attribute id=\"0x11f4e\">
                  <value> #{alertEndTime} </value>
                </attribute>
              </less-than>
            </and>
          </filtered-models>
          </search-criteria>
        </rs:attribute-filter>
        #{@attr_of_interest}
        </rs:alarm-request>"

        # Post to Spectrum and parse results
        begin
          res=resource.post @xml,:content_type => 'application/xml',:accept => 'application/json'
        rescue
          $log.warn $!.to_s
          $log.warn "Spectrum Input :: could not poll alarms from spectrum"
          res = nil
        end 

        if !res.nil? && !res.body.nil?
          begin           
            body = JSON.parse(res.body)
            pollingEnd = Time.parse(res.headers[:date]).to_i
            pollingDuration = Engine.now.to_i - pollingStart
          rescue
            $log.warn $!.to_s
            $log.warn "Spectrum Input :: could not parse message body to json properly: #{res.body.to_s}"
            body = nil
          end
        else
          body = nil
        end

        
        if !body.nil?   # if body is not nil, then try to transform and emit it
          begin
            # Processing for alerts returned
            $log.info "Spectrum Input :: returned #{body['ns1.alarm-response-list']['@total-alarms'].to_i} alarms for period >= #{alertStartTime.to_i} and < #{alertEndTime.to_i} took #{pollingDuration.to_i} seconds"
            if body['ns1.alarm-response-list']['@total-alarms'].to_i > 0
              ns1_alarm = body['ns1.alarm-response-list']['ns1.alarm-responses']['ns1.alarm']
              if ns1_alarm .is_a?(Array)  # mulitple alarms
                $log.info "Spectrum Input :: ns1.alarm is an array of size #{ns1_alarm.size}"
                # iterate through each alarm
                ns1_alarm .each do |alarm|
                  parse_and_emit_the_alarm(alarm, pollingEnd)
                end
                @highwatermark.update_records(alertEndTime,@state_tag) 
              elsif ns1_alarm .is_a?(Hash) # single alarm
                $log.info "Spectrum Input :: ns1.alarm is a hash for single alarm"
                parse_and_emit_the_alarm(ns1_alarm, pollingEnd)
                @highwatermark.update_records(alertEndTime,@state_tag) 
              else 
                $log.warn "Spectrum Input :: ns1.alarm is of unexpected type, not hash or array"
              end              
            end                    
          rescue 
            $log.warn $!.to_s
            $log.warn "Spectrum Input :: could not transform and emit the message body properly: #{body.to_s} "
          end
        end # end of body.nil?
      end
    end # def on_timer
  end # class SpectrumInput
end # module Fluent