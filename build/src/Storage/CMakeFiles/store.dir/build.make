# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.27

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:

#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:

# Disable VCS-based implicit rules.
% : %,v

# Disable VCS-based implicit rules.
% : RCS/%

# Disable VCS-based implicit rules.
% : RCS/%,v

# Disable VCS-based implicit rules.
% : SCCS/s.%

# Disable VCS-based implicit rules.
% : s.%

.SUFFIXES: .hpux_make_needs_suffix_list

# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

#Suppress display of executed commands.
$(VERBOSE).SILENT:

# A target that is always out of date.
cmake_force:
.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/rain/stard

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/rain/stard/build

# Include any dependencies generated for this target.
include src/Storage/CMakeFiles/store.dir/depend.make
# Include any dependencies generated by the compiler for this target.
include src/Storage/CMakeFiles/store.dir/compiler_depend.make

# Include the progress variables for this target.
include src/Storage/CMakeFiles/store.dir/progress.make

# Include the compile flags for this target's objects.
include src/Storage/CMakeFiles/store.dir/flags.make

src/Storage/CMakeFiles/store.dir/BufferManager.cpp.o: src/Storage/CMakeFiles/store.dir/flags.make
src/Storage/CMakeFiles/store.dir/BufferManager.cpp.o: /home/rain/stard/src/Storage/BufferManager.cpp
src/Storage/CMakeFiles/store.dir/BufferManager.cpp.o: src/Storage/CMakeFiles/store.dir/compiler_depend.ts
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --green --progress-dir=/home/rain/stard/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object src/Storage/CMakeFiles/store.dir/BufferManager.cpp.o"
	cd /home/rain/stard/build/src/Storage && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -MD -MT src/Storage/CMakeFiles/store.dir/BufferManager.cpp.o -MF CMakeFiles/store.dir/BufferManager.cpp.o.d -o CMakeFiles/store.dir/BufferManager.cpp.o -c /home/rain/stard/src/Storage/BufferManager.cpp

src/Storage/CMakeFiles/store.dir/BufferManager.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --green "Preprocessing CXX source to CMakeFiles/store.dir/BufferManager.cpp.i"
	cd /home/rain/stard/build/src/Storage && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/rain/stard/src/Storage/BufferManager.cpp > CMakeFiles/store.dir/BufferManager.cpp.i

src/Storage/CMakeFiles/store.dir/BufferManager.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --green "Compiling CXX source to assembly CMakeFiles/store.dir/BufferManager.cpp.s"
	cd /home/rain/stard/build/src/Storage && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/rain/stard/src/Storage/BufferManager.cpp -o CMakeFiles/store.dir/BufferManager.cpp.s

# Object files for target store
store_OBJECTS = \
"CMakeFiles/store.dir/BufferManager.cpp.o"

# External object files for target store
store_EXTERNAL_OBJECTS =

src/Storage/libstore.a: src/Storage/CMakeFiles/store.dir/BufferManager.cpp.o
src/Storage/libstore.a: src/Storage/CMakeFiles/store.dir/build.make
src/Storage/libstore.a: src/Storage/CMakeFiles/store.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --green --bold --progress-dir=/home/rain/stard/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX static library libstore.a"
	cd /home/rain/stard/build/src/Storage && $(CMAKE_COMMAND) -P CMakeFiles/store.dir/cmake_clean_target.cmake
	cd /home/rain/stard/build/src/Storage && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/store.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
src/Storage/CMakeFiles/store.dir/build: src/Storage/libstore.a
.PHONY : src/Storage/CMakeFiles/store.dir/build

src/Storage/CMakeFiles/store.dir/clean:
	cd /home/rain/stard/build/src/Storage && $(CMAKE_COMMAND) -P CMakeFiles/store.dir/cmake_clean.cmake
.PHONY : src/Storage/CMakeFiles/store.dir/clean

src/Storage/CMakeFiles/store.dir/depend:
	cd /home/rain/stard/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/rain/stard /home/rain/stard/src/Storage /home/rain/stard/build /home/rain/stard/build/src/Storage /home/rain/stard/build/src/Storage/CMakeFiles/store.dir/DependInfo.cmake "--color=$(COLOR)"
.PHONY : src/Storage/CMakeFiles/store.dir/depend

