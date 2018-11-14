import com.atguigu.model.UserCityStatModel;
import com.atguigu.service.BehaviorStatService;
import com.atguigu.utils.PropertiesUtils;
import com.atguigu.utils.StringUtil;

import java.io.IOException;
import java.util.Properties;

public class HBaseAdd {

    public static void main(String[] args) {

        Properties prop = null;
            prop = PropertiesUtils.getProperties();

        BehaviorStatService service = BehaviorStatService.getInstance(prop);

        UserCityStatModel model = new UserCityStatModel();
        model.setCity("Xinjiang");
        model.setUserId(StringUtil.getFixedLengthStr("10", 10));

        for (int i = 0; i <3 ; i++){
            service.addUserNumOfCity(model);
        }
    }
}
