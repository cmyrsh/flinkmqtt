package mtch.flinkmqtt.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.protobuf.ProtobufFactory;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by mchaubal on 1/6/18.
 */
public class FormatManager<OUT> implements Serializable{

    public enum AVAILABLE_FORMATS {XML, JSON, AVRO, PROTOBUF};

    private ObjectMapper mapper;

    private final Class<OUT> genericClass;



    public FormatManager(AVAILABLE_FORMATS format, Class<OUT> outclass) throws UnsupportedOperationException{
        if(!contains(format.toString())) {
            throw new UnsupportedOperationException(
                    String.format("Data format %s not recognized. Available values %s%n",
                            format, AVAILABLE_FORMATS.values()));

        }
            switch (format) {
                case  XML: mapper = new XmlMapper(); break;
                case JSON : mapper = new ObjectMapper(); break;
                case AVRO : mapper = new AvroMapper(); break;
                case PROTOBUF : mapper = new ObjectMapper(new ProtobufFactory());

        }

        this.genericClass = outclass;
    }

    public OUT build(byte[] data) throws IOException {
        return mapper.readValue(data, genericClass);
    }



    private final boolean contains(String test) {

        for (AVAILABLE_FORMATS c : AVAILABLE_FORMATS.values()) {
            if (c.name().equals(test)) {
                return true;
            }
        }

        return false;
    }

    private final void throwError(String message) {
        throw new UnsupportedOperationException(message);
    }
}
