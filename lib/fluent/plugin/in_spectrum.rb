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
        raise ConfigError, "Spectrum :: ConfigError 'endpoint' and 'username' and 'password' must be all specified."
      end
      # Enforce min interval
      if @interval.to_i < INTERVAL_MIN
        raise ConfigError, "Spectrum :: ConfigError 'interval' must be #{INTERVAL_MIN} or over."
      end
      # Warn about optional state file
      unless @state_type == "file" || @state_type =="redis"
        $log.warn "Spectrum :: 'state_type' is not set to file or redis"
        $log.warn "Spectrum :: state file or redis are recommended to save the last known good timestamp to resume event consuming"
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
        "0x10009" => "SECURITY_STRING",
        "0xffff00f6" => "CUSTOM_APPLICATION_NAME",
        "0xffff00f7" => "CUSTOM_BUSINESS_UNIT_I2",
        "0xffff00f8" => "CUSTOM_BUSINESS_UNIT_I3",
        "0xffff00f9" => "CUSTOM_BUSINESS_UNIT_I4",
        "0xffff00fa" => "CUSTOM_CMDB_CI_SYSTEM"
      }


      # Create XML chunk for attributes we care about
      @attr_of_interest=""
      if(@attributes.upcase === "__ALL__")
        $log.info "Spectrum :: all attributes"
        @spectrum_access_code.each do |key, value|
          $log.info "key: #{key},  value: #{value}"
          @attr_of_interest += " <rs:requested-attribute id=\"#{key}\"/>"
        end
      else
        $log.info "Spectrum :: selected attributes"
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
            raise ConfigError, "Spectrum :: ConfigError attribute '#{attr}' is not in the hash map"
          end
          $log.info "key: #{key},  value: #{value}"
          @attr_of_interest += " <rs:requested-attribute id=\"#{key}\"/>"
        }
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

    def on_timer
      if not @stop_flag
        pollingStart = Engine.now.to_i
        if @highwatermark.last_records(@state_tag)
          alertStartTime = @highwatermark.last_records(@state_tag)
          $log.info "Spectrum :: got hwm form state file: #{alertStartTime.to_i}"
        else
          alertStartTime = (pollingStart.to_i - @interval.to_i)
          $log.info "Spectrum :: no hwm, got new alert start time: #{alertStartTime.to_i}"
        end
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
            <greater-than>
              <attribute id=\"0x11f4e\">
                <value> #{alertStartTime} </value>
              </attribute>
            </greater-than>
          </filtered-models>
          </search-criteria>
        </rs:attribute-filter>
        #{@attr_of_interest}
        </rs:alarm-request>"

        # Post to Spectrum and parse results
        begin
          res=resource.post @xml,:content_type => 'application/xml',:accept => 'application/json'
          body = JSON.parse(res.body)
          pollingEnd = Time.parse(res.headers[:date]).to_i
          pollingDuration = Engine.now.to_i - pollingStart
        end  

        # Processing for multiple alerts returned
        if body['ns1.alarm-response-list']['@total-alarms'].to_i > 1
          $log.info "Spectrum :: returned #{body['ns1.alarm-response-list']['@total-alarms'].to_i} alarms for period < #{alertStartTime.to_i} took #{pollingDuration.to_i} seconds, ended at #{pollingEnd}"
          # iterate through each alarm
          body['ns1.alarm-response-list']['ns1.alarm-responses']['ns1.alarm'].each do |alarm|
            # Create initial structure
            record_hash = Hash.new # temp hash to hold attributes of alarm
            raw_array = Array.new # temp hash to hold attributes of alarm for raw
            record_hash['event_type'] = @tag.to_s
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

            # argos specific code:
            # if bu_l4 is not null, means it's already processed by argos
            bu_l4 = record_hash['CUSTOM_BUSINESS_UNIT_I4']
            if (bu_l4.nil? || bu_l4.empty?) # the alert is new, not processed by argos yet
              record_hash['CUSTOM_BUSINESS_UNIT_I4'] = 'alert.raw.spectrum'
            else
              record_hash['CUSTOM_BUSINESS_UNIT_I4'] = 'alert.processed.spectrum'
            end
            # end of argos specific code

            Engine.emit(@tag, record_hash['CREATION_DATE'].to_i,record_hash)
          end
        # Processing for single alarm returned  
        elsif body['ns1.alarm-response-list']['@total-alarms'].to_i == 1
          $log.info "Spectrum :: returned #{body['ns1.alarm-response-list']['@total-alarms'].to_i} alarms for period < #{alertStartTime.to_i} took #{pollingDuration.to_i} seconds, ended at #{pollingEnd}"
          # Create initial structure
          record_hash = Hash.new # temp hash to hold attributes of alarm
          raw_array = Array.new # temp hash to hold attributes of alarm for raw
          record_hash['event_type'] = @tag.to_s
          record_hash['intermediary_source'] = @endpoint.to_s
          record_hash['receive_time_input'] = pollingEnd.to_s
          # iterate though alarm attributes and add to temp hash  
          body['ns1.alarm-response-list']['ns1.alarm-responses']['ns1.alarm']['ns1.attribute'].each do |attribute|
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

          # argos specific code:
          # if bu_l4 is not null, means it's already processed by argos
          bu_l4 = record_hash['CUSTOM_BUSINESS_UNIT_I4']
          if (bu_l4.nil? || bu_l4.empty?) # the alert is new, not processed by argos yet
            record_hash['CUSTOM_BUSINESS_UNIT_I4'] = 'alert.raw.spectrum'
          else
            record_hash['CUSTOM_BUSINESS_UNIT_I4'] = 'alert.processed.spectrum'
          end
          # end of argos specific code

          Engine.emit(@tag, record_hash['CREATION_DATE'].to_i,record_hash)
        # No alarms returned
        else
          $log.info "Spectrum :: returned #{body['ns1.alarm-response-list']['@total-alarms'].to_i} alarms for period < #{alertStartTime.to_i} took #{pollingDuration.to_i} seconds, ended at #{pollingEnd}"
        end
        @highwatermark.update_records(pollingEnd,@state_tag)
      end
    end # def input
  end # class SpectrumInput
end # module Fluent