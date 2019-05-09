package system;

import com.mendix.core.actionmanagement.IActionRegistrator;

public class UserActionsRegistrar
{
  public void registerActions(IActionRegistrator registrator)
  {
    registrator.bundleComponentLoaded();
    registrator.registerUserAction(oql.actions.AddBooleanParameter.class);
    registrator.registerUserAction(oql.actions.AddDateTimeParameter.class);
    registrator.registerUserAction(oql.actions.AddDecimalParameter.class);
    registrator.registerUserAction(oql.actions.AddIntegerLongValue.class);
    registrator.registerUserAction(oql.actions.AddObjectParameter.class);
    registrator.registerUserAction(oql.actions.AddStringParameter.class);
    registrator.registerUserAction(oql.actions.CountRowsOQLStatement.class);
    registrator.registerUserAction(oql.actions.ExecuteOQLStatement.class);
    registrator.registerUserAction(oql.actions.ExportOQLToCSV.class);
    registrator.registerUserAction(system.actions.VerifyPassword.class);
  }
}
