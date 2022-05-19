#include <kspp/serdes/proto_serdes.h>
namespace kspp {

static void write_varint(int64_t data, std::vector<uint8_t>* v){
  do {
    uint8_t next_byte = (data & 0x7F);
    data >>= 7;
    // Add continue bit.
    if (data) next_byte |= 0x80;
    v->push_back(next_byte & 0xFF);
  } while (data);
}
//borrowed with pride from
//https://github.com/confluentinc/confluent-kafka-dotnet/blob/master/src/Confluent.SchemaRegistry.Serdes.Protobuf/ProtobufSerializer.cs
std::vector<uint8_t> create_index_array(const google::protobuf::Message *m) {
  std::vector<uint8_t> indices;
  auto md = m->GetDescriptor();
  auto current = md;
  while (current->containing_type()) {
    auto prev = current;
    current = current->containing_type();
    bool found_nested = false;

    for (int i = 0; i < current->nested_type_count(); ++i) {
      // this code is justa guess - csharp used ClrType
      // if (md.File.MessageTypes[i].ClrType == currentMd.ClrType)  ??
      if (current->nested_type(i)->index() == prev->index()) {
        indices.push_back(i);
        found_nested = true;
        break;
      }
    }
    if (!found_nested) {
      throw std::runtime_error("Invalid message descriptor nesting.");
    }
  }

  // Add the index of the root MessageDescriptor in the FileDescriptor.
  bool found_descriptor = false;
  for (int i = 0; i < md->file()->message_type_count(); ++i) {
    //if (md.File.MessageTypes[i].ClrType == currentMd.ClrType)  ???
    if (md->file()->message_type(i)->index() == current->index()) {
      indices.push_back(i);
      found_descriptor = true;
      break;
    }
  }
  if (!found_descriptor) {
    throw std::runtime_error("MessageDescriptor not found.");
  }

  // optimization for the special case [0]
  if (indices.size() == 1 && indices[0] == 0)
    return indices;

  std::vector<uint8_t> result;
  // this is zigzag encoded WriteVarint from back to front
  write_varint(indices.size(), &result);
  for (size_t i = 0; i != indices.size(); ++i) {
    write_varint((uint)indices[indices.size() - i - 1], &result);
  }
  return result;
}

} // namespace kspp

/*private static byte[] createIndexArray(MessageDescriptor md, bool
   useDeprecatedFormat)
        {
            var indices = new List<int>();

            // Walk the nested MessageDescriptor tree up to the root.
            var currentMd = md;
            while (currentMd.ContainingType != null)
            {
                var prevMd = currentMd;
                currentMd = currentMd.ContainingType;
                bool foundNested = false;
                for (int i=0; i<currentMd.NestedTypes.Count; ++i)
                {
                    if (currentMd.NestedTypes[i].ClrType == prevMd.ClrType)
                    {
                        indices.Add(i);
                        foundNested = true;
                        break;
                    }
                }
                if (!foundNested)
                {
                    throw new InvalidOperationException("Invalid message
   descriptor nesting.");
                }
            }

            // Add the index of the root MessageDescriptor in the
   FileDescriptor. bool foundDescriptor = false; for (int i=0;
   i<md.File.MessageTypes.Count; ++i)
            {
                if (md.File.MessageTypes[i].ClrType == currentMd.ClrType)
                {
                    indices.Add(i);
                    foundDescriptor = true;
                    break;
                }
            }
            if (!foundDescriptor)
            {
                throw new InvalidOperationException("MessageDescriptor not
   found.");
            }

            using (var result = new MemoryStream())
            {
                if (indices.Count == 1 && indices[0] == 0)
                {
                    // optimization for the special case [0]
                    result.WriteByte(0);
                }
                else
                {
                    if (useDeprecatedFormat)
                    {
                        result.WriteUnsignedVarint((uint)indices.Count);
                    }
                    else
                    {
                        result.WriteVarint((uint)indices.Count);
                    }
                    for (int i=0; i<indices.Count; ++i)
                    {
                        if (useDeprecatedFormat)
                        {
                            result.WriteUnsignedVarint((uint)indices[indices.Count-i-1]);
                        }
                        else
                        {
                            result.WriteVarint((uint)indices[indices.Count-i-1]);
                        }
                    }
                }

                return result.ToArray();
            }
        }
*/