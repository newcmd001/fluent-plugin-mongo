module Fluent


class MongoBatchOutput < MongoOutput
  Fluent::Plugin.register_output('mongo_batch', self)

  def format(tag, time, record)
    [time, record].to_msgpack
  end

  def emit(tag, es, chain)
    # TODO: Should replacement using eval in configure?
    # Fluent::Engine.emit(tag, time, record)
	$log.warn "tag: " + tag
    if @tag_mapped
      super(tag, es, chain, tag)
    else
      super(tag, es, chain)
    end
  end

  def write(chunk)
    # TODO: See emit comment
    collection_name = @tag_mapped ? chunk.key : @collection
    if @sogamo
      collection_name_array = collection_name.split(".", 4)
      collection_name = collection_name_array[0]
      collection_name << "."
      collection_name << collection_name_array[1]
    end
    if @date_mapped
      time1 = Time.new
      time_str = time1.strftime(@time_slice_format)
      collection_name << time_str
    end
    if @sogamo
      collection_name << "."
      collection_name << collection_name_array[2]
      collection_index = collection_name_array[3]
    end
    operate(get_or_create_collection(collection_name, collection_index), collect_records(chunk))
  end

  private

  def operate(collection, records)
    begin
      record_ids, error_records = collection.insert(records, INSERT_ARGUMENT)
      if !@ignore_invalid_record and error_records.size > 0
        operate_invalid_records(collection, error_records)
      end
    rescue Mongo::OperationFailure => e
      # Probably, all records of _records_ are broken...
      if e.error_code == 13066  # 13066 means "Message contains no documents"
        operate_invalid_records(collection, records) unless @ignore_invalid_record
      else
        raise e
      end
    end
    records
  end

  def operate_invalid_records(collection, records)
    converted_records = records.map { |record|
      new_record = {}
      new_record[@tag_key] = record.delete(@tag_key) if @include_tag_key
      new_record[@time_key] = record.delete(@time_key)
      new_record[BROKEN_DATA_KEY] = BSON::Binary.new(Marshal.dump(record))
      new_record
    }
    collection.insert(converted_records) # Should create another collection like name_broken?
  end

  def collect_records(chunk)
    records = []
    chunk.msgpack_each { |time, record|
      record[@time_key] = Time.at(time || record[@time_key]) if @include_time_key
      records << record
    }
    records
  end

  FORMAT_COLLECTION_NAME_RE = /(^\.+)|(\.+$)/

  def format_collection_name(collection_name)
    formatted = collection_name
    formatted = formatted.gsub(@remove_tag_prefix, '') if @remove_tag_prefix
    formatted = formatted.gsub(FORMAT_COLLECTION_NAME_RE, '')
    formatted = @collection if formatted.size == 0 # set default for nil tag
    formatted
  end

  def get_or_create_collection(collection_name, collection_index)
    collection_name = format_collection_name(collection_name)
    return @clients[collection_name] if @clients[collection_name]

    @db ||= get_connection
    if @db.collection_names.include?(collection_name)
      collection = @db.collection(collection_name)
      unless @disable_collection_check
        unless @collection_options[:capped] == collection.capped? # TODO: Verify capped configuration
          raise ConfigError, "New configuration is different from existing collection"
        end
      end
    else
      collection = @db.create_collection(collection_name, @collection_options)
      collection.create_index(collection_index)
    end

    @clients[collection_name] = collection
  end

  def get_connection
    db = Mongo::Connection.new(@host, @port, @connection_options).db(@database)
    authenticate(db)
  end

  # Following limits are heuristic. BSON is sometimes bigger than MessagePack and JSON.
  LIMIT_BEFORE_v1_8 = 2 * 1024 * 1024  # 2MB  = 4MB  / 2
  LIMIT_AFTER_v1_8 = 10 * 1024 * 1024  # 10MB = 16MB / 2 + alpha

end


end
