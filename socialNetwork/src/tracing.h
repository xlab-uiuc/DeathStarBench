#include <utility>

#ifndef SOCIAL_NETWORK_MICROSERVICES_TRACING_H
#define SOCIAL_NETWORK_MICROSERVICES_TRACING_H

#include <string>
#include <yaml-cpp/yaml.h>
#include <jaegertracing/Tracer.h>

// #include <opentelemetry/exporters/otlp/otlp_http_exporter.h>
// #include <opentelemetry/exporters/memory/in_memory_span_exporter.h>
#include "opentelemetry/exporters/ostream/span_exporter_factory.h"
#include "opentelemetry/sdk/trace/exporter.h"
#include "opentelemetry/sdk/trace/processor.h"
#include "opentelemetry/sdk/trace/simple_processor_factory.h"
#include "opentelemetry/sdk/trace/tracer_provider_factory.h"
// #include <opentelemetry/sdk/trace/simple_processor.h>
// #include <opentelemetry/sdk/trace/samplers/always_on.h>
#include "opentelemetry/trace/provider.h"

#include <opentracing/propagation.h>
#include <string>
#include <map>
#include "logger.h"

namespace sdktrace = opentelemetry::sdk::trace;

namespace social_network {

using opentracing::expected;
using opentracing::string_view;

class TextMapReader : public opentracing::TextMapReader {
 public:
  explicit TextMapReader(const std::map<std::string, std::string> &text_map)
      : _text_map(text_map) {}

  expected<void> ForeachKey(
      std::function<expected<void>(string_view key, string_view value)> f)
  const override {
    for (const auto& key_value : _text_map) {
      auto result = f(key_value.first, key_value.second);
      if (!result) return result;
    }
    return {};
  }

 private:
  const std::map<std::string, std::string>& _text_map;
};

class TextMapWriter : public opentracing::TextMapWriter {
 public:
  explicit TextMapWriter(std::map<std::string, std::string> &text_map)
    : _text_map(text_map) {}

  expected<void> Set(string_view key, string_view value) const override {
    _text_map[key] = value;
    return {};
  }

 private:
  std::map<std::string, std::string>& _text_map;
};

void SetUpTracer(
    const std::string &config_file_path,
    const std::string &service) {
  auto configYAML = YAML::LoadFile(config_file_path);

  // Enable local Jaeger agent, by prepending the service name to the default
  // Jaeger agent's hostname
  // configYAML["reporter"]["localAgentHostPort"] = service + "-" +
  //     configYAML["reporter"]["localAgentHostPort"].as<std::string>();

  auto config = jaegertracing::Config::parse(configYAML);

  bool r = false;
  while (!r) {
    try
    {
      auto tracer = jaegertracing::Tracer::make(
        service, config, jaegertracing::logging::consoleLogger());
      r = true;
      opentracing::Tracer::InitGlobal(
      std::static_pointer_cast<opentracing::Tracer>(tracer));
    }
    catch(...)
    {
      LOG(error) << "Failed to connect to jaeger, retrying ...";
      sleep(1);
    }
  }


}

void SetUpOpenTelemetryTracer(const std::string &service) {
    // Create OTLP HTTP exporter
    // // auto exporter = std::unique_ptr<opentelemetry::sdk::trace::SpanExporter>(
    // //     new opentelemetry::exporters::otlp::OtlpGrpcExporter());
    // opentelemetry::exporter::otlp::OtlpHttpExporterOptions opts;
    // opts.url = "http://localhost:4318/v1/traces";
    // auto exporter = std::unique_ptr<sdktrace::SpanExporter>(
    //     new opentelemetry::exporter::otlp::OtlpHttpExporter(opts));

    // memory exporter
    // auto memory_exporter = std::unique_ptr<sdktrace::SpanExporter>(new opentelemetry::exporter::memory::InMemorySpanExporter);
    auto exporter  = opentelemetry::exporter::trace::OStreamSpanExporterFactory::Create();

    // Create simple span processor
    auto processor = sdktrace::SimpleSpanProcessorFactory::Create(std::move(exporter));

    // //AlwaysOnSampler
    // auto always_on_sampler = std::unique_ptr<sdktrace::AlwaysOnSampler>(
    //     new sdktrace::AlwaysOnSampler);
    
    // auto tracer_context = std::make_shared<sdktrace::TracerContext>(
    //     std::move(processor), resource, std::move(always_on_sampler));

    // Create tracer provider with the simple processor
    // auto provider = std::shared_ptr<opentelemetry::trace::TracerProvider>(
    //     new sdktrace::TracerProvider(std::move(processor)));
    std::shared_ptr<opentelemetry::trace::TracerProvider> provider =
      sdktrace::TracerProviderFactory::Create(std::move(processor));

    // Set as global tracer provider
    opentelemetry::trace::Provider::SetTracerProvider(provider);
}

} //namespace social_network

#endif //SOCIAL_NETWORK_MICROSERVICES_TRACING_H
