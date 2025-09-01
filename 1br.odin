package main

import "core:terminal"
import "core:thread"
import "core:fmt"
import "core:os"
import "core:time"
import "core:sys/info"
import "core:strconv"
import "core:sync"

Thread_Data :: struct {
    chunk: []byte,
    result: map[string]Calc_Result,
}

Calc_Result :: struct {
    station: string,
    min_temp: f32,
    max_temp: f32,
    avg_temp: f32,
}



main :: proc() {
    if len(os.args) != 2 {
        fmt.println("Usage: 1br <data_file>")
        return
    }
    file_path := os.args[1]
    stopwatch: time.Stopwatch
    num_procs := info.cpu.logical_cores
    fmt.println("1BR Challenge")
    fmt.println("CPU Count:", num_procs)
    fmt.println("Reading data from", file_path)

    time.stopwatch_start(&stopwatch)

    bytes, read_ok := os.read_entire_file(file_path)
    if !read_ok {
        fmt.println("Error reading file: ", file_path)
        return
    }
    log_time("read file", stopwatch)
    fmt.println("file read successfully, size:", len(bytes))


    results := chunk_and_process(bytes, num_procs)
    // results := calculate_stats(bytes)
    log_time("total", stopwatch)
    fmt.println("completed calculations")

    // output the results to results.txt
    // one station per line w/ format:
    // <station-name>=<min-temp>/<max-temp>/<avg-temp>

    write_flags := os.O_RDWR | os.O_CREATE | os.O_TRUNC
    mode := os.S_IRUSR | os.S_IWUSR | os.S_IRGRP | os.S_IROTH
    file, err := os.open("results.txt", write_flags, mode)
    if err != os.ERROR_NONE {
        fmt.println("Error opening results.txt")
        return
    }
    defer os.close(file)
    for station, result in results {
        os.write_string(file, fmt.tprintf("%s=%.1f/%.1f/%.1f\n", station, result.min_temp, result.max_temp, result.avg_temp))
    }

}

chunk_and_process :: proc(bytes: []byte, num_procs: int) -> map[string]Calc_Result {

    input_size := len(bytes)
    chunk_size := input_size / num_procs
    chunk_beg := 0
    chunk_end := 0
    thread_datas: [dynamic]Thread_Data
    // split the input into chunks, ensuring that each chunk end is the end of a line
    for i := 0; i < num_procs; i += 1 {
        // first just offset by chunk_size
        chunk_end = min(chunk_beg + chunk_size, input_size - 1)
        // then find the next line
        for ; chunk_end < input_size - 1;{
            char := bytes[chunk_end]
            if char == '\n' {
                break
            }
            chunk_end += 1
        }
        chunk_slice := bytes[chunk_beg:chunk_end]
        chunk_beg = chunk_end + 1
        data := Thread_Data{chunk_slice, map[string]Calc_Result{}}
        append(&thread_datas, data)
    }


    threads: [dynamic]^thread.Thread
    for i := 0; i < num_procs; i += 1 {
        t := thread.create(chunk_thread_proc)
        t.user_index = i
        t.data = &thread_datas[i]
        append(&threads, t)
        thread.start(t)
    }


    thread.join_multiple(..threads[:])
    for t in threads {
        thread.destroy(t)
    }

    merged_results := merge_results(thread_datas[:])
    return merged_results
}

merge_results :: proc(thread_datas: []Thread_Data) -> map[string]Calc_Result {
    merged_results := map[string]Calc_Result{}
    for data in thread_datas {
        for station, result in data.result {
            merged_result, ok := merged_results[station]
            if !ok {
                merged_results[station] = result
            } else {
                merged_result.min_temp = min(merged_result.min_temp, result.min_temp)
                merged_result.max_temp = max(merged_result.max_temp, result.max_temp)
                merged_result.avg_temp = (merged_result.avg_temp + result.avg_temp) / 2
            }
        }
    }
    return merged_results
}

chunk_thread_proc :: proc(t: ^thread.Thread) {
    data := cast(^Thread_Data)t.data
    fmt.println("starting chunk thread:", t.user_index)
    data.result = calculate_stats(data.chunk)
    fmt.println("completed chunk thread:", t.user_index)
}



calculate_stats :: proc(bytes: []byte) -> map[string]Calc_Result {
    // parse line by line
    // each line has the format:
    // <station-name>;<temp>
    // we want to calculate the min, max, and avg temp for each station
    station := ""
    station_beg := 0
    temp_beg := 0
    results := map[string]Calc_Result{}
    for char, index in bytes {
        if char == ';' {
            station = string(bytes[station_beg:index])
            temp_beg = index + 1
            continue
        }
        if char == '\n' {
            temp_f32, f_ok := parse_temp(bytes[temp_beg:index])
            if !f_ok {
                fmt.println("Error parsing temp:", string(bytes[temp_beg:index]))
                continue
            }
            station_data, s_ok := results[station]
            if !s_ok {
                station_data = Calc_Result{station, temp_f32, temp_f32, temp_f32}
            } else {
                station_data.min_temp = min(station_data.min_temp, temp_f32)
                station_data.max_temp = max(station_data.max_temp, temp_f32)
                station_data.avg_temp = (station_data.avg_temp + temp_f32) / 2
            }
            results[station] = station_data
            station = ""
            station_beg = index + 1
            continue
        }
    }
    return results
}

parse_temp :: proc(temp_bytes: []byte) -> (f32, bool) {
    if len(temp_bytes) == 0 {
        return 0, false
    }
    i := 0
    sign := f32(1)
    if temp_bytes[0] == '-' {
        sign = -1
        i = 1
    }

    whole_part := 0
    decimal_part := 0

    // parse the whole part base 10, using ascii math
    for i < len(temp_bytes) && temp_bytes[i] != '.' {
        if temp_bytes[i] < '0' || temp_bytes[i] > '9' {
            return 0, false
        }
        whole_part = whole_part * 10 + int(temp_bytes[i] - '0')
        i += 1
    }

    // skip the decimal point
    if i >= len(temp_bytes) || temp_bytes[i] != '.' {
        return 0, false
    }
    i += 1
    // parse the decimal part (one digit)
    if i >= len(temp_bytes) || temp_bytes[i] < '0' || temp_bytes[i] > '9' {
        return 0, false
    }
    decimal_part = int(temp_bytes[i] - '0')
    i += 1

    if i != len(temp_bytes) {
        return 0, false
    }
    res := f32(whole_part) + f32(decimal_part) * 0.1
    return sign * res, true
}

log_time :: proc(tag: string, stopwatch: time.Stopwatch) {
    fmt.printf("%s - elapsed: %v\n", tag, time.stopwatch_duration(stopwatch))
}


