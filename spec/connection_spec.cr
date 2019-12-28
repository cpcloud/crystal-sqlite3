require "./spec_helper"


class Avg
  def initialize(@total = 0.0, @count = 0_u64)
  end

  def step(value : Float64?) : Void
    if !value.nil?
      @total += value
      @count += 1
    end
  end

  def final : Float64?
    count = @count
    return nil if !count
    result = @total / count
  end
end

class WeightedAvg
  def initialize(@numerator = 0.0, @denominator = 0.0, @count = 0_u64)
  end

  def step(value : Float64?, weight : Float64?) : Void
    if !value.nil? && !weight.nil?
      @numerator += value * weight
      @denominator += weight
      @count += 1
    end
  end

  def final : Float64?
    puts "count == #{@count}"
    return nil if !@count
    puts "numerator == #{@numerator}"
    puts "denominator == #{@denominator}"
    @numerator / @denominator
  end
end

class StringLengthSum
  def initialize(@total_length = 0_i64, @count = 0_u64)
  end

  def step(value : String?) : Void
    if !value.nil?
			@total_length += value.size
      @count += 1
    end
  end

  def final : Int64?
    return nil if !@count
		@total_length
  end
end

private def dump(source, target)
  source.using_connection do |conn|
    conn = conn.as(SQLite3::Connection)
    target.using_connection do |backup_conn|
      backup_conn = backup_conn.as(SQLite3::Connection)
      conn.dump(backup_conn)
    end
  end
end

describe Connection do
  it "opens a database and then backs it up to another db" do
    with_db do |db|
      with_db("./test2.db") do |backup_db|
        db.exec "create table person (name text, age integer)"
        db.exec "insert into person values (\"foo\", 10)"

        dump db, backup_db

        backup_name = backup_db.scalar "select name from person"
        backup_age = backup_db.scalar "select age from person"
        source_name = db.scalar "select name from person"
        source_age = db.scalar "select age from person"

        {backup_name, backup_age}.should eq({source_name, source_age})
      end
    end
  end

  it "opens a database, inserts records, dumps to an in-memory db, inserts some more, then dumps to the source" do
    with_db do |db|
      with_mem_db do |in_memory_db|
        db.exec "create table person (name text, age integer)"
        db.exec "insert into person values (\"foo\", 10)"
        dump db, in_memory_db

        in_memory_db.scalar("select count(*) from person").should eq(1)
        in_memory_db.exec "insert into person values (\"bar\", 22)"
        dump in_memory_db, db

        db.scalar("select count(*) from person").should eq(2)
      end
    end
  end

  it "opens a database, inserts records (>1024K), and dumps to an in-memory db" do
    with_db do |db|
      with_mem_db do |in_memory_db|
        db.exec "create table person (name text, age integer)"
        db.transaction do |tx|
          100_000.times { tx.connection.exec "insert into person values (\"foo\", 10)" }
        end
        dump db, in_memory_db
        in_memory_db.scalar("select count(*) from person").should eq(100_000)
      end
    end
  end

  it "opens a connection without the pool" do
    with_cnn do |cnn|
      cnn.should be_a(SQLite3::Connection)

      cnn.exec "create table person (name text, age integer)"
      cnn.exec "insert into person values (\"foo\", 10)"

      cnn.scalar("select count(*) from person").should eq(1)
    end
  end

  it "creates, registers and runs two numeric aggregate functions" do
    with_cnn do |cnn|
      cnn.exec "create table person (age float, weight float)"
      cnn.exec "insert into person values (10, 1), (20, 1)"
      create_aggregate(cnn, :my_avg, Avg, true)
      create_aggregate(cnn, :my_wavg, WeightedAvg, true)
      #cnn.scalar("select my_avg(age) from person").should eq(15.0)
      cnn.scalar("select my_wavg(age, weight) from person").should eq(15.0)
    end
  end

  it "creates, registers and runs a string aggregate function" do
    with_cnn do |cnn|
      cnn.exec "create table person (name text)"
      cnn.exec "insert into person values ('bob'), ('joe'), ('alice')"
      create_aggregate(cnn, :my_string_length_sum, StringLengthSum, false)
      cnn.scalar("select my_string_length_sum(name) from person").should eq(11)
    end
  end

  it "creates, registers and runs a string scalar function" do

    with_cnn do |cnn|
      cnn.exec "create table person (name text)"
      cnn.exec "insert into person values ('bob'), ('joe'), ('alice')"
			create_function(
				cnn,
				def my_string_length(value : String?) : Int64?
					value.try &.size
				end
			)
      cnn.scalar("select sum(my_string_length(name)) from person").should eq(11)
    end
  end
end
