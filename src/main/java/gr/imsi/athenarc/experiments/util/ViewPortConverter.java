package gr.imsi.athenarc.experiments.util;

import com.beust.jcommander.IStringConverter;
import gr.imsi.athenarc.middleware.domain.ViewPort;

public class ViewPortConverter implements IStringConverter<ViewPort> {
    @Override
    public ViewPort convert(String value) {
        return new ViewPort(Integer.parseInt(value.split(",")[0]), Integer.parseInt(value.split(",")[1]));
    }
}
