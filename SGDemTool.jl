using Dates
using DataFrames, CSV
using Distributions

##### defining structs #####

"""
Struct taking different values for summer and winter seasons
"""
mutable struct demand
    summer::AbstractVector
    winter::AbstractVector
end

"""
Struct for data on each appliance
"""
mutable struct appliance
    name::String
    wattage::Float64
    duration::demand
    start::demand
end

"""
Struct for appliance data on each household
"""
mutable struct household
    name::String
    appliances::AbstractVector
end

##### helping functions #####

"""
Draw appliance start from a uniform distribution depending on the length of the day segment
"""
function sample_start(i::Int64)
    
    # sampling for segments of the day from uniform distribution
    if 0 < i < length(segments_duration[:,1])
        s = rand(1:segments_duration[i,2]) - 1
        return s
    
    # no sampling if aplliance not in use
    elseif i == 7
        s = 0
        return s
    
    else
        error("out of range")
    end
end

"""
Draw appliance uptime duration from a truncated normal distribution
"""
function sample_duration(i::Int64)
    
    # sampling from truncated normal distribution with mean at max duration and truncation at mean
    if i > 0
        d = round(rand(TruncatedNormal(i,1,0,i)),digits=0)
        return d
    
    # no sampling if appliance not in use
    elseif i == 0
        d = 0
        return d
    else
        error("out of range")
    end
end

"""
Determine type of day (weekday, Friday, Saturday)
"""
function test_day(day::Date)

    # weekday
    if 0 < dayofweek(day) < 5 || dayofweek(day) == 7
        daytype = 1
    
    # Friday
    elseif dayofweek(day) == 5
        daytype = 2

    # Saturday
    elseif dayofweek(day) == 6
        daytype = 3
    
    else
        error("out of bounds")
    end
    return daytype
end

"""
Generate an hourly timecode between a starting day s and an ending day e
"""
function generate_timecode(s::Date,e::Date)
    # add hours 00:00 to 23:00
    ts_time_int = s+Time(0):Hour(1):e+Time(23)
    ts_timecode = collect(ts_time_int)
    return ts_timecode
end

##### main functions #####

"""
Generate starting and stopping hour for an appliance a for a specific day d
"""
function app_duration(a::appliance,d::Date)

    # determine start and stop in summer
    if seasons[month(d)] == 1 
        start = d + Time(Dates.Hour(segments_start[a.start.summer[test_day(d)],2] + sample_start(a.start.summer[test_day(d)])))
        stop = start + Dates.Hour(sample_duration(a.duration.summer[test_day(d)]))
        return start, stop

    # determine start and stop in winter
    elseif seasons[month(d)] == 2
        start = d + Time(Dates.Hour(segments_start[a.start.summer[test_day(d)],2] + sample_start(a.start.winter[test_day(d)])))
        stop = start + Dates.Hour(sample_duration(a.duration.winter[test_day(d)]))
        return start, stop
    
    else
        error("out of bounds")
    end
end

"""
Generate load time series for an appliance a between a starting day s and an ending day e with hourly timecode tc
"""
function generate_app_load_ts(a::appliance,s::Date,e::Date,tc::Vector)
    
    # initialize dataframe with time code
    app_load_ts = DataFrame()
    app_load_ts.Timecode = tc
    app_load_ts[!, :load] .= 0.0

    # add hourly device wattage between start and stop
    for i in collect(s:Day(1):e)
        start_stop = app_duration(a,i)
        for i in 1:length(tc)
            if start_stop[1] <= tc[i] <= start_stop[2]
                app_load_ts.load[i] = app_load_ts.load[i] + a.wattage
            end
        end
    end

    return app_load_ts
end

"""
Generate load time series for a specific household hh based on a starting day s and an ending day e
"""
function generate_hh_load_ts(hh::household,s::Date,e::Date)
    
    # initialize dataframe with time code
    hh_load_ts = DataFrame()
    hh_load_ts.Timecode = generate_timecode(s,e)

    # simulate every device
    for i in 1:length(hh.appliances)
        if hh.appliances[i] == 1
            hh_load_ts.appl = generate_app_load_ts(appl[i],s,e,hh_load_ts.Timecode).load
            rename!(hh_load_ts,:appl => appl[i].name)
        elseif hh.appliances[i] > 1
            hh_load_ts.appl = generate_app_load_ts(appl[i],s,e,hh_load_ts.Timecode).load
            for n in 2:hh.appliances[i]
                hh_load_ts.appl = hh_load_ts.appl + (generate_app_load_ts(appl[i],s,e,hh_load_ts.Timecode).load * rand(d))
            end
            rename!(hh_load_ts,:appl => appl[i].name)
        end
    end
    
    # add one column of total demand (sum) to dataframe
    hh_load_ts.total_load = sum(eachcol(hh_load_ts[!,2:end]))
    return hh_load_ts
end

"""
Generate load time series for a swarm grid based on a start day s and an end day e
"""
function generate_sg_load_ts(s::Date,e::Date)

    # initialize dataframe with time code
    sg_load_ts = DataFrame()
    sg_load_ts.Timecode = generate_timecode(s,e)

    # simulate every household
    for i in 1:length(hh)
        @info "simulating household $(hh[i].name)"
        sg_load_ts.hh = generate_hh_load_ts(hh[i],start_ts,end_ts).total_load
        rename!(sg_load_ts,:hh => hh[i].name)
    end

    return sg_load_ts
end

##### loading data #####

# appliance data
appl_dat = CSV.File("_input/data_appliances.csv") |> DataFrame
appl = []

for i in 1:nrow(appl_dat)
    push!(appl,appliance(appl_dat.name[i],appl_dat.wattage[i]/1000,demand([appl_dat.h_sum_wd[i],appl_dat.h_sum_fri[i],appl_dat.h_sum_sat[i]],[appl_dat.h_win_wd[i],appl_dat.h_win_fri[i],appl_dat.h_win_sat[i]]),demand([appl_dat.s_sum_wd[i],appl_dat.s_sum_fri[i],appl_dat.s_sum_sat[i]],[appl_dat.s_win_wd[i],appl_dat.s_win_fri[i],appl_dat.s_win_sat[i]])))
end

# household data
hh_dat = CSV.File("_input/data_households.csv") |> DataFrame
hh = []

for i in 1:nrow(hh_dat)
    push!(hh,household(hh_dat[i,1],Array(hh_dat[i,2:end])))
end

# define segments of the day
"""
# 1 = night (00-06)
# 2 = morning (06-09)
# 3 = late morning (09-12)
# 4 = afternoon (12-15)
# 5 = late afternoon (15-18)
# 6 = evening (18-00)
# 7 = not in use
"""
segments_duration = [1 6;2 3;3 3;4 3;5 3;6 6; 7 0]
segments_start = [1 0;2 6;3 9;4 12;5 15;6 18; 7 0]

# define seasons of the months
"""
1 = summer
2 = winter
"""
seasons = [1,1,1,1,2,2,2,2,2,1,1,1]

# distribution and probability of use for cases where more than one of the same appliance exists in a household
p = 0.3
d = Binomial(1,p)

##### generate smart grid load profile #####

start_ts = Date("2019-06-01") # start day of time series
end_ts = Date("2019-06-07") # end day of time series

sg_load_ts = generate_sg_load_ts(start_ts,end_ts)

CSV.write("_output\\sg_load_ts-$(start_ts)_$(end_ts).csv", sg_load_ts)

##### plotting results #####

using Plots
using TimeSeries

sg_load_ta = TimeArray(sg_load_ts, timestamp = :Timecode) # convert dataframe to timearray
plot(sg_load_ta)
savefig("_output\\sg_load_ts-$(start_ts)_$(end_ts).png")