# Build the SIP gateway binary (PJSUA2-based).
PJPROJECT_DIR ?= $(HOME)/Projects/pjproject

include $(PJPROJECT_DIR)/build.mak

APP = sip_gateway
OBJS = core/sip_gateway.o media/udp_audio_port.o util/port_allocator.o control/control_channel.o
DEPS := $(OBJS:.o=.d)

CXX := $(PJ_CXX)
CXXFLAGS := $(PJ_CXXFLAGS) -std=c++17 -MMD -MP
LDFLAGS := $(PJ_LDXXFLAGS)
LDLIBS := $(PJ_LDXXLIBS)

.PHONY: all clean

all: $(APP)

$(APP): $(OBJS)
	$(CXX) -o $@ $(OBJS) $(LDFLAGS) $(LDLIBS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -I. -c $< -o $@

clean:
	rm -f $(APP) $(OBJS) $(DEPS)

-include $(DEPS)
