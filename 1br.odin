package main

import "base:runtime"
import "core:fmt"
import "core:os"
import "core:sort"
import "core:strconv"
import "core:strings"
import "core:sys/info"
import "core:sys/posix"
import "core:thread"
import "core:time"

USE_MMAP :: #config(USE_MMAP, true)
O_FLAGS :: os.O_RDWR | os.O_CREATE | os.O_TRUNC
O_MODE :: os.S_IRUSR | os.S_IWUSR | os.S_IRGRP | os.S_IROTH

Thread_Data :: struct {
	chunk:  []byte,
	result: map[string]Calc_Result,
}

Calc_Result :: struct {
	min_temp:   f32,
	max_temp:   f32,
	sum_temp:   f32,
	count_temp: int,
	station:    string,
}

Key_Value :: struct {
	key:   string,
	value: Calc_Result,
}


main :: proc() {
	if len(os.args) < 2 {
		fmt.println("Usage: 1br <data_file> [optional: <num_procs>]")
		return
	}
	file_path := os.args[1]
	num_jobs := info.cpu.logical_cores
	if len(os.args) == 3 {
		num_jobs = strconv.parse_int(os.args[2]) or_else num_jobs
	}

	fmt.println("1BR Challenge")
	fmt.println("USE_MMAP:", USE_MMAP)
	fmt.println("Job Count:", num_jobs)

	sw: time.Stopwatch
	time.stopwatch_start(&sw)

	when USE_MMAP {
		fd, oerr := os.open(file_path, os.O_RDONLY)
		if oerr != os.ERROR_NONE {
			fmt.println("Error opening file", file_path)
			return
		}
		defer os.close(fd)

		file_size, fserr := os.file_size(fd)
		if fserr != os.ERROR_NONE {
			fmt.println("Error getting file size", file_path)
			return
		}
		fmt.println("File size", file_size)
		when ODIN_OS == .Darwin || ODIN_OS == .Linux {
			bytes_ptr: rawptr = posix.mmap(
				nil,
				uint(file_size),
				{.READ},
				{.SHARED},
				posix.FD(fd),
				0,
			)
			defer posix.munmap(bytes_ptr, uint(file_size))
			bytes := ([^]byte)(bytes_ptr)[:file_size]
			run_1br(bytes, num_jobs)
		} else {
			fmt.println("Unsupported OS", ODIN_OS)
			fmt.println("Please feel free to add support for your OS")
			return
		}
	} else {

		file_size, fserr := os.file_size(file_path)
		if fserr != os.ERROR_NONE {
			fmt.println("Error getting file size", file_path)
			return
		}
		fmt.printfln("File size %d bytes", file_size)
		bytes, rd_ok := os.read_entire_file(file_path)
		if !rdok {
			fmt.println("Error reading file", file_path)
			return
		}
		run_1br(bytes, num_jobs)
	}

	fmt.println("Finished in", time.stopwatch_duration(sw))
}

run_1br :: proc(bytes: []byte, num_jobs: int) {
	// sw: time.Stopwatch
	// time.stopwatch_start(&sw)

	results := num_jobs == 1 ? calculate_stats(bytes) : chunk_and_process(bytes, num_jobs)

	// fmt.println("file processing duration:", time.stopwatch_duration(sw))
	// time.stopwatch_reset(&sw)
	// time.stopwatch_start(&sw)

	sorted_results := sort_results(results)
	delete(results)

	// fmt.println("sorting duration:", time.stopwatch_duration(sw))
	// time.stopwatch_reset(&sw)
	// time.stopwatch_start(&sw)

	output := write_to_str(sorted_results[:])
	delete(sorted_results)

	// fmt.println("writing to string duration:", time.stopwatch_duration(sw))
	// time.stopwatch_reset(&sw)
	// time.stopwatch_start(&sw)

	write_to_file("results.txt", output)

	// fmt.println("writing to file duration:", time.stopwatch_duration(sw))
}

calculate_stats :: proc(bytes: []byte) -> map[string]Calc_Result #no_bounds_check {
	// parse line by line
	// each line has the format:
	// <station-name>;<temp>
	// we want to calculate the min, max, and avg temp for each station
	station: string
	station_beg := 0
	temp_beg := 0
	results := make(map[string]Calc_Result)

	for char, index in bytes {
		if char == ';' {
			station = transmute(string)bytes[station_beg:index]
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
				station_data = Calc_Result{temp_f32, temp_f32, temp_f32, 1, station}
			} else {
				station_data.min_temp = min(station_data.min_temp, temp_f32)
				station_data.max_temp = max(station_data.max_temp, temp_f32)
				station_data.sum_temp += temp_f32
				station_data.count_temp += 1
			}
			results[station] = station_data
			station = ""
			station_beg = index + 1
			continue
		}
	}

	return results
}

parse_temp :: proc(temp_bytes: []byte) -> (f32, bool) #no_bounds_check {
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

chunk_and_process :: proc(bytes: []byte, num_jobs: int) -> map[string]Calc_Result #no_bounds_check {
	// sw := time.Stopwatch{}
	// time.stopwatch_start(&sw)

	input_size := len(bytes)
	chunk_size := input_size / num_jobs
	chunk_beg := 0
	chunk_end := 0
	thread_datas := make([dynamic]Thread_Data, 0, num_jobs)
	defer delete(thread_datas)
	// split the input into chunks, ensuring that each chunk end is the end of a line
	for i := 0; i < num_jobs; i += 1 {
		// first just offset by chunk_size
		chunk_end = min(chunk_beg + chunk_size, input_size - 1)
		// then find the next line
		for chunk_end < input_size - 1 {
			char := bytes[chunk_end]
			if char == '\n' {
				break
			}
			chunk_end += 1
		}
		chunk_slice := bytes[chunk_beg:chunk_end]
		chunk_beg = chunk_end + 1
		data: Thread_Data
		data.chunk = chunk_slice
		data.result = make(map[string]Calc_Result)
		append(&thread_datas, data)
	}

	thread_proc :: proc(t: ^thread.Thread) {
		data := cast(^Thread_Data)t.data
		data.result = calculate_stats(data.chunk)
	}


	threads := make([dynamic]^thread.Thread, 0, num_jobs)
	defer delete(threads)
	for i := 0; i < num_jobs; i += 1 {
		t := thread.create(thread_proc)
		t.init_context = context
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
	for data in thread_datas {
		delete(data.result)
	}
	return merged_results
}

merge_results :: proc(thread_datas: []Thread_Data) -> map[string]Calc_Result {
	merged_results := make(map[string]Calc_Result)

	for data in thread_datas {
		for station, result in data.result {
			merged_result, ok := merged_results[station]
			if !ok {
				merged_results[station] = result
			} else {
				merged_result.min_temp = min(merged_result.min_temp, result.min_temp)
				merged_result.max_temp = max(merged_result.max_temp, result.max_temp)
				merged_result.sum_temp += result.sum_temp
				merged_result.count_temp += result.count_temp
			}
		}
	}

	return merged_results
}

sort_results :: proc(results: map[string]Calc_Result) -> [dynamic]Key_Value {
	out := make([dynamic]Key_Value)
	for station, result in results {
		append(&out, Key_Value{station, result})
	}
	sort.quick_sort_proc(out[:], proc(a, b: Key_Value) -> int {
		return strings.compare(a.key, b.key)
	})
	return out
}

write_to_file :: proc(file_path: string, output: string) {
	file, err := os.open(file_path, O_FLAGS, O_MODE)
	if err != os.ERROR_NONE {
		fmt.println("Error opening", file_path)
		return
	}
	defer os.close(file)
	os.write_string(file, output)
}

write_to_str :: proc(values: []Key_Value) -> string {
	// single line, enclosed in {}. station names sorted alphabetically
	// {<station-name>=<min-temp>/<avg-temp>/<max-temp>...}
	sb := strings.builder_make()
	strings.write_string(&sb, "{")
	for entry, index in values {
		avg_temp := entry.value.sum_temp / f32(entry.value.count_temp)
		strings.write_string(&sb, entry.key)
		strings.write_rune(&sb, '=')
		strings.write_string(
			&sb,
			fmt.tprintf("%.1f/%.1f/%.1f", entry.value.min_temp, avg_temp, entry.value.max_temp),
		)
		if index != len(values) - 1 {
			strings.write_string(&sb, ", ")
		}
	}
	strings.write_string(&sb, "}")
	return strings.to_string(sb)
}
