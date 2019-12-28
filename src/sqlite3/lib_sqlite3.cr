require "./type"

@[Link("sqlite3")]
lib LibSQLite3
  type SQLite3 = Void*
  type Statement = Void*
  type SQLite3Backup = Void*
  type SQLite3Context = Void*
  type SQLite3Value = Void*

  type ScalarFunc = (SQLite3Context, Int32, SQLite3Value*) ->
  type StepFunc = (SQLite3Context, Int32, SQLite3Value*) ->
  type FinalizeFunc = SQLite3Context ->
  type ValueFunc = SQLite3Context ->
  type InverseFunc = (SQLite3Context, Int32, SQLite3Value*) ->
	alias Destructor = Void* ->

  enum Code
    OKAY =   0
    ROW  = 100
    DONE = 101
  end

  alias Callback = (Void*, Int32, UInt8**, UInt8**) -> Int32

  fun open_v2 = sqlite3_open_v2(filename : UInt8*, db : SQLite3*, flags : ::SQLite3::Flag, zVfs : UInt8*) : Int32

  fun errcode = sqlite3_errcode(SQLite3) : Int32
  fun errmsg = sqlite3_errmsg(SQLite3) : UInt8*

  fun backup_init = sqlite3_backup_init(SQLite3, UInt8*, SQLite3, UInt8*) : SQLite3Backup
  fun backup_step = sqlite3_backup_step(SQLite3Backup, Int32) : Code
  fun backup_finish = sqlite3_backup_finish(SQLite3Backup) : Code

  fun prepare_v2 = sqlite3_prepare_v2(db : SQLite3, zSql : UInt8*, nByte : Int32, ppStmt : Statement*, pzTail : UInt8**) : Int32
  fun step = sqlite3_step(stmt : Statement) : Int32
  fun column_count = sqlite3_column_count(stmt : Statement) : Int32
  fun column_type = sqlite3_column_type(stmt : Statement, iCol : Int32) : ::SQLite3::Type
  fun column_int64 = sqlite3_column_int64(stmt : Statement, iCol : Int32) : Int64
  fun column_double = sqlite3_column_double(stmt : Statement, iCol : Int32) : Float64
  fun column_text = sqlite3_column_text(stmt : Statement, iCol : Int32) : UInt8*
  fun column_bytes = sqlite3_column_bytes(stmt : Statement, iCol : Int32) : Int32
  fun column_blob = sqlite3_column_blob(stmt : Statement, iCol : Int32) : UInt8*

  fun bind_int = sqlite3_bind_int(stmt : Statement, idx : Int32, value : Int32) : Int32
  fun bind_int64 = sqlite3_bind_int64(stmt : Statement, idx : Int32, value : Int64) : Int32
  fun bind_text = sqlite3_bind_text(stmt : Statement, idx : Int32, value : UInt8*, bytes : Int32, destructor : Void* ->) : Int32
  fun bind_blob = sqlite3_bind_blob(stmt : Statement, idx : Int32, value : UInt8*, bytes : Int32, destructor : Void* ->) : Int32
  fun bind_null = sqlite3_bind_null(stmt : Statement, idx : Int32) : Int32
  fun bind_double = sqlite3_bind_double(stmt : Statement, idx : Int32, value : Float64) : Int32

  fun bind_parameter_index = sqlite3_bind_parameter_index(stmt : Statement, name : UInt8*) : Int32
  fun reset = sqlite3_reset(stmt : Statement) : Int32
  fun column_name = sqlite3_column_name(stmt : Statement, idx : Int32) : UInt8*
  fun last_insert_rowid = sqlite3_last_insert_rowid(db : SQLite3) : Int64
  fun changes = sqlite3_changes(db : SQLite3) : Int32

  fun finalize = sqlite3_finalize(stmt : Statement) : Int32
  fun close_v2 = sqlite3_close_v2(SQLite3) : Int32
  fun close = sqlite3_close(SQLite3) : Int32

  fun aggregate_context = sqlite3_aggregate_context(ctx : SQLite3Context, nBytes : Int32) : Void*
  fun create_function = sqlite3_create_function(
    db : SQLite3, name : UInt8*, nArg : Int32, eTextRep : Int32, pApp : Void*,
    xFunc : ScalarFunc, xStep : StepFunc, xFinal : FinalizeFunc
  ) : Int32

  fun value_type = sqlite3_value_type(value : SQLite3Value) : Int32
  fun value_int = sqlite3_value_int(value : SQLite3Value) : Int32
  fun value_int64 = sqlite3_value_int64(value : SQLite3Value) : Int64
  fun value_double = sqlite3_value_double(value : SQLite3Value) : Float64
  fun value_blob = sqlite3_value_blob(SQLite3Value) : Void*
  fun value_text = sqlite3_value_text(SQLite3Value) : UInt8*
  fun value_bytes = sqlite3_value_bytes(SQLite3Value) : Int32

  fun result_null = sqlite3_result_null(ctx : SQLite3Context)
  fun result_int = sqlite3_result_int(ctx : SQLite3Context, result : Int32)
  fun result_int64 = sqlite3_result_int64(ctx : SQLite3Context, result : Int64)
  fun result_double = sqlite3_result_double(ctx : SQLite3Context, result : Float64)
  fun result_text = sqlite3_result_text(ctx : SQLite3Context,
                                        chars : UInt8*,
                                        nchars : Int32,
                                        destructor : Void* ->)
  fun result_blob = sqlite3_result_blob(ctx : SQLite3Context,
                                        bytes : Void*,
                                        nbytes : Int32,
                                        destructor : Void* ->)
  fun user_data = sqlite3_user_data(ctx : SQLite3Context) : Void*
  NULL          =     5
  UTF8          =     1
  DETERMINISTIC = 0x800
	TRANSIENT = Pointer(Void).new(-1).unsafe_as(LibSQLite3::Destructor)
	STATIC = Pointer(Void).new(0).unsafe_as(LibSQLite3::Destructor)
end

macro create_function(db, func, deterministic)
	{{ func }}
	{% args = func.args %}
	{% nargs = args.size %}
  LibSQLite3.create_function(
    {{ db }},
    "#{{{ func }}}",
		{{ nargs }},
    LibSQLite3::UTF8 | ({{ deterministic }} ? LibSQLite3::DETERMINISTIC : 0),
    nil,
    ->(ctx : LibSQLite3::SQLite3Context, argc : Int32, argv : LibSQLite3::SQLite3Value*) {
      args = {
        {% for i in 0...nargs %}
          if LibSQLite3.value_type(argv[{{ i }}]) == LibSQLite3::NULL
            nil
          else
            {% type = args[i].restriction.types[0].resolve %}
            {% if type == Float64 %}
              LibSQLite3.value_double(argv[{{ i }}])
            {% elsif type == Int64 %}
              LibSQLite3.value_int64(argv[{{ i }}])
            {% elsif type == Int32 %}
              LibSQLite3.value_int(argv[{{ i }}])
            {% elsif type == String %}
              String.new(LibSQLite3.value_text(argv[{{ i }}]),
                         LibSQLite3.value_bytes(argv[{{ i }}]))
            {% end %}
          end,
        {% end %}
      }

      result = {{ func }}(*args)

			if !result.nil?
				{% type = func.name.return_type.types[0].resolve %}
				{% if type == Float64 %}
					LibSQLite3.result_double(ctx, result)
				{% elsif type == Int64 %}
					LibSQLite3.result_int64(ctx, result)
				{% elsif type == Int32 %}
					LibSQLite3.result_int(ctx, result)
				{% elsif type == String %}
					LibSQLite3.result_text(
						ctx, result.to_unsafe, result.size, LibSQLite3::TRANSIENT)
				{% end %}
			else
				LibSQLite3.result_null(ctx)
			end
    },
    nil,
		nil,
  )
end


macro create_aggregate(db, name, cls, deterministic)
  LibSQLite3.create_function(
    {{ db }},
    {{ name }}.to_s.to_unsafe,
    {{ cls.resolve.methods.find { |m| m.name == "step" }.args.size }},
    LibSQLite3::UTF8 | ({{ deterministic }} ? LibSQLite3::DETERMINISTIC : 0),
    nil,
    nil,
    ->(ctx : LibSQLite3::SQLite3Context, argc : Int32, argv : LibSQLite3::SQLite3Value*) {
      nbytes = instance_sizeof({{ cls }})
      raw = LibSQLite3.aggregate_context(ctx, nbytes)
      if !raw.null?
        {% type = cls.resolve %}
        {% method = type.methods.find { |m| m.name == "step" } %}
        {% args = method.args %}
        args = {
          {% for i in 0...(args.size) %}
            if LibSQLite3.value_type(argv[{{ i }}]) == LibSQLite3::NULL
              nil
            else
              {% type = args[i].restriction.types[0].resolve %}
              {% if type == Float64 %}
                LibSQLite3.value_double(argv[{{ i }}])
              {% elsif type == Int64 %}
                LibSQLite3.value_int64(argv[{{ i }}])
              {% elsif type == Int32 %}
                LibSQLite3.value_int(argv[{{ i }}])
              {% elsif type == String %}
                String.new(LibSQLite3.value_text(argv[{{ i }}]),
                           LibSQLite3.value_bytes(argv[{{ i }}]))
              {% end %}
            end,
          {% end %}
        }
				agg_ctx = raw.unsafe_as({{ cls }})
        agg_ctx.step(*args)
      end
    },
    ->(ctx : LibSQLite3::SQLite3Context) {
      nbytes = instance_sizeof({{ cls }})
      raw = LibSQLite3.aggregate_context(ctx, nbytes)
      if !raw.null?
				agg_ctx = raw.unsafe_as({{ cls }})
        result = agg_ctx.final
        if !result.nil?
          {% finalize = cls.resolve.methods.find { |m| m.name == "final" } %}
          {% type = finalize.return_type.types[0].resolve %}
          {% if type == Float64 %}
            LibSQLite3.result_double(ctx, result)
          {% elsif type == Int64 %}
            LibSQLite3.result_int64(ctx, result)
          {% elsif type == Int32 %}
            LibSQLite3.result_int(ctx, result)
          {% elsif type == String %}
            LibSQLite3.result_text(
							ctx, result.to_unsafe, result.size, LibSQLite3::STATIC)
          {% end %}
        else
          LibSQLite3.result_null(ctx)
        end
      end
    },
  )
end
