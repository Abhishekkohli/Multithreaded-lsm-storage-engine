# Makefile for Log-Structured Storage Engine
# 
# Build targets:
#   make          - Build release version
#   make debug    - Build with debug symbols
#   make clean    - Remove build artifacts
#   make test     - Run tests

# Compiler and flags
CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -Wpedantic -I./include

# Release flags
RELEASE_FLAGS = -O3 -DNDEBUG

# Debug flags
DEBUG_FLAGS = -g -O0 -DDEBUG -fsanitize=address,undefined

# Linker flags
LDFLAGS = -pthread

# Directories
SRC_DIR = src
INC_DIR = include
OBJ_DIR = obj
BIN_DIR = bin

# Source files
SOURCES = $(wildcard $(SRC_DIR)/*.cpp)
OBJECTS = $(patsubst $(SRC_DIR)/%.cpp,$(OBJ_DIR)/%.o,$(SOURCES))

# Exclude main.cpp for library
LIB_SOURCES = $(filter-out $(SRC_DIR)/main.cpp,$(SOURCES))
LIB_OBJECTS = $(patsubst $(SRC_DIR)/%.cpp,$(OBJ_DIR)/%.o,$(LIB_SOURCES))

# Output binary
TARGET = $(BIN_DIR)/storage_engine

# Default target
all: release

# Release build
release: CXXFLAGS += $(RELEASE_FLAGS)
release: directories $(TARGET)
	@echo "Release build complete: $(TARGET)"

# Debug build
debug: CXXFLAGS += $(DEBUG_FLAGS)
debug: LDFLAGS += -fsanitize=address,undefined
debug: directories $(TARGET)
	@echo "Debug build complete: $(TARGET)"

# Create output directories
directories:
	@mkdir -p $(OBJ_DIR) $(BIN_DIR)

# Link the final executable
$(TARGET): $(OBJECTS)
	$(CXX) $(OBJECTS) -o $@ $(LDFLAGS)

# Compile source files
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Dependencies (headers)
$(OBJ_DIR)/common.o: $(INC_DIR)/common.h
$(OBJ_DIR)/thread_pool.o: $(INC_DIR)/thread_pool.h $(INC_DIR)/common.h
$(OBJ_DIR)/memtable.o: $(INC_DIR)/memtable.h $(INC_DIR)/common.h
$(OBJ_DIR)/wal.o: $(INC_DIR)/wal.h $(INC_DIR)/common.h
$(OBJ_DIR)/sstable.o: $(INC_DIR)/sstable.h $(INC_DIR)/common.h $(INC_DIR)/memtable.h
$(OBJ_DIR)/compaction.o: $(INC_DIR)/compaction.h $(INC_DIR)/sstable.h $(INC_DIR)/thread_pool.h
$(OBJ_DIR)/storage_engine.o: $(INC_DIR)/storage_engine.h $(INC_DIR)/memtable.h \
                              $(INC_DIR)/wal.h $(INC_DIR)/sstable.h $(INC_DIR)/compaction.h
$(OBJ_DIR)/main.o: $(INC_DIR)/storage_engine.h

# Clean build artifacts
clean:
	rm -rf $(OBJ_DIR) $(BIN_DIR)
	rm -rf data/
	@echo "Clean complete"

# Clean and rebuild
rebuild: clean all

# Run the demo
run: release
	./$(TARGET)

# Run with debug build
run-debug: debug
	./$(TARGET)

# Static analysis with cppcheck (if installed)
check:
	@which cppcheck > /dev/null 2>&1 && \
		cppcheck --enable=all --std=c++17 -I$(INC_DIR) $(SRC_DIR)/*.cpp 2>&1 | \
		grep -v "Cppcheck cannot find" || \
		echo "cppcheck not installed"

# Format code with clang-format (if installed)
format:
	@which clang-format > /dev/null 2>&1 && \
		find $(SRC_DIR) $(INC_DIR) -name "*.cpp" -o -name "*.h" | \
		xargs clang-format -i || \
		echo "clang-format not installed"

# Show lines of code
loc:
	@echo "Lines of code:"
	@wc -l $(SRC_DIR)/*.cpp $(INC_DIR)/*.h | tail -1

# Help
help:
	@echo "Log-Structured Storage Engine Build System"
	@echo ""
	@echo "Targets:"
	@echo "  all (default)  - Build release version"
	@echo "  release        - Build optimized release version"
	@echo "  debug          - Build with debug symbols and sanitizers"
	@echo "  clean          - Remove build artifacts"
	@echo "  rebuild        - Clean and rebuild"
	@echo "  run            - Build and run release version"
	@echo "  run-debug      - Build and run debug version"
	@echo "  check          - Run static analysis (requires cppcheck)"
	@echo "  format         - Format code (requires clang-format)"
	@echo "  loc            - Count lines of code"
	@echo "  help           - Show this message"

.PHONY: all release debug directories clean rebuild run run-debug check format loc help
