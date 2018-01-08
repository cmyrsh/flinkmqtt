package mtch.flinkmqtt.util;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;

/**
 * Created by mchaubal on 1/6/18.
 */
public class FormatManager<OUT, T extends ObjectMapper> implements Serializable{

    private final ObjectMapper mapper;

    private final Class<OUT> outClass;



    public FormatManager() throws InstantiationException, IllegalAccessException{

        mapper = Builder.createInstance(getClassOfT());

        outClass = getClassOfOUT();
    }

    public OUT build(byte[] data) throws IOException {
        return mapper.readValue(data, outClass);
    }


    private static class Builder {
        static <T> T createInstance(Class clazz) throws InstantiationException, IllegalAccessException{
            T t = (T) clazz.newInstance();
            return t;
        }
    }

    private final Class<OUT> getClassOfOUT() {
        return (Class<OUT>)  ((ParameterizedType)getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }
    private final Class<T> getClassOfT() {
        return (Class<T>)  ((ParameterizedType)getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }
}
