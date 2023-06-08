using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace DataFeederSparkplug
{
	static class Program
	{
		/// <summary>
		/// The main entry point for the application.
		/// </summary>
		static void Main(string[] args)
		{
			if (Environment.UserInteractive)
			{
				DataFeederSparkplug service1 = new DataFeederSparkplug(); // Add args to () if needed
				service1.TestStartupAndStop(args);
			}
			else
			{
				ServiceBase[] ServicesToRun;
				ServicesToRun = new ServiceBase[]
				{
					new DataFeederSparkplug()
				};
				ServiceBase.Run(ServicesToRun);
			}
		}
	}
}
