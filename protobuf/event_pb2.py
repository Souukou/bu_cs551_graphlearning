# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: event.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0b\x65vent.proto\x1a\x1fgoogle/protobuf/timestamp.proto\"\xb4\x01\n\x05\x45vent\x12-\n\ttimestamp\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x0e\n\x06source\x18\x02 \x01(\x05\x12\x0e\n\x06target\x18\x03 \x01(\x05\x12\x14\n\x0csource_label\x18\x04 \x01(\x05\x12\x14\n\x0ctarget_label\x18\x05 \x01(\x05\x12\x17\n\x0fsource_data_hex\x18\x06 \x01(\t\x12\x17\n\x0ftarget_data_hex\x18\x07 \x01(\tB%\n\x14graphlearning.protosB\x0b\x45ventProtosP\x01\x62\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'event_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'\n\024graphlearning.protosB\013EventProtosP\001'
  _EVENT._serialized_start=49
  _EVENT._serialized_end=229
# @@protoc_insertion_point(module_scope)
