using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace TestSite
{
	public partial class _Default : System.Web.UI.Page
	{
		protected void Page_Load(object sender, EventArgs e)
		{
			var a = Session["A"];
			var b = (a == null) ? 1 : (int)a;

			Session["A"] = b + 1;
		}
	}
}
