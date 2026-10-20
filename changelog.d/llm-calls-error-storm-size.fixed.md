- An Anthropic error record in llm-calls keeps the full request only the
  first time that log sees the request fail. A repeat, such as each retry of
  a rate limit or an overload, records the request's size under
  `rawRequestOmitted`, and `requestSha256` on each error record points a
  repeat at the copy written. One OAuth 429 storm had grown a single log to
  687MB. A request membrane rejects before sending (such as an unsupported
  prefill) is now kept too, from the error. `LLM_CALLS_FULL_PAYLOADS=1` still
  keeps every copy.
