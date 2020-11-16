using MigrateEmailAndPhoneCallWPF.UserControls;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace MigrateEmailAndPhoneCallWPF
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();

            #region Optimize Connection settings

            //Change max connections from .NET to a remote service default: 2
            System.Net.ServicePointManager.DefaultConnectionLimit = 65000;
            //Bump up the min threads reserved for this app to ramp connections faster - minWorkerThreads defaults to 4, minIOCP defaults to 4
            System.Threading.ThreadPool.SetMinThreads(100, 100);
            //Turn off the Expect 100 to continue message - 'true' will cause the caller to wait until it round-trip confirms a connection to the server
            System.Net.ServicePointManager.Expect100Continue = false;
            //Can decreas overall transmission overhead but can cause delay in data packet arrival
            System.Net.ServicePointManager.UseNagleAlgorithm = false;

            #endregion Optimize Connection settings
        }

        private void BtnPhoneCall_Click(object sender, RoutedEventArgs e)
        {
            btnEmail.IsChecked = false;
            btnAppointment.IsChecked = false;
            btnTask.IsChecked = false;

            PhoneCallUC pc = new PhoneCallUC();
            rbGridLeft.Children.Clear();
            rbGridLeft.Children.Add(pc);
        }

        private void BtnEmail_Click(object sender, RoutedEventArgs e)
        {          
            btnPhoneCall.IsChecked = false;
            btnAppointment.IsChecked = false;
            btnTask.IsChecked = false;

            EmailUC em = new EmailUC();
            rbGridLeft.Children.Clear();
            rbGridLeft.Children.Add(em);
        }

        private void TbMinRibb_Checked(object sender, RoutedEventArgs e)
        {
            RibbonMinimize();
        }

        private void TbMinRibb_Unchecked(object sender, RoutedEventArgs e)
        {
            RibbonMaximize();
        }

        private void RbMain_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            RibbonMaximize();
        }

        private void RibbonMinimize()
        {
            rbMain.IsMinimized = true;
            tbMinRibb.SmallImageSource = new BitmapImage(new Uri(@"Images/Arrow_down.png", UriKind.Relative));
        }

        private void RibbonMaximize()
        {
            rbMain.IsMinimized = false;
            tbMinRibb.SmallImageSource = new BitmapImage(new Uri(@"Images/arrow-up-2.png", UriKind.Relative));
        }

        private void BtnTask_Click(object sender, RoutedEventArgs e)
        {
            btnPhoneCall.IsChecked = false;
            btnEmail.IsChecked = false;
            btnAppointment.IsChecked = false;

            TaskUC t = new TaskUC();
            rbGridLeft.Children.Clear();
            rbGridLeft.Children.Add(t);
        }

        private void BtnAppointment_Click(object sender, RoutedEventArgs e)
        {
            btnPhoneCall.IsChecked = false;
            btnEmail.IsChecked = false;
            btnTask.IsChecked = false;

            AppointmentUC app = new AppointmentUC();
            rbGridLeft.Children.Clear();
            rbGridLeft.Children.Add(app);
        }
    }
}
