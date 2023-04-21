using System;
using System.Text;
using System.Security;
using System.Security.Cryptography;
using System.IO;

namespace DataFeederSparkplug
{
	class UserCredStore
	{
		// user name entropy combined with this constant
		private static byte[] additionalEntropy = new byte[] { 0x45, 0xF3, 0x10, 0xD3 };

		public static bool FileReadCredentials(string CredFile, out string User, out SecureString Password)
		{
			string CredentialsString = "";
			User = "";
			Password = new SecureString();
			try
			{
				StreamReader CredFileReader = new StreamReader(CredFile);
				CredentialsString = CredFileReader.ReadLine();
				CredFileReader.Close();
			}
			catch (Exception e)
			{
				Console.WriteLine("Unable to read credentials file. " + e.Message);
				return false;
			}
			var Credentials = CredentialsString.Split(',');
			if (Credentials.Length == 2)
			{
				Console.WriteLine("Read credentials from file: " + CredFile);
				return DecryptUserCreds(Credentials[0], Credentials[1], out User, out Password);
			}
			User = "";
			Password = new SecureString();
			return false;
		}

		public static bool FileWriteCredentials( string CredFile, string User, string Password)
		{
			EncryptCreds(User, Password, out string EncUser, out string EncPassword);

			try
			{
				StreamWriter CredFileWriter = new StreamWriter(CredFile);
				CredFileWriter.WriteLine(EncUser + "," + EncPassword);
				CredFileWriter.Close();
			}
			catch (Exception e)
			{
				Console.WriteLine("Unable to write credentials file: " + e.Message);
				return false;
			}
			return true;
		}
		private static bool EncryptCreds(string txtUser, string txtPassword, out string encUser, out string encPassword)
		{
			byte[] userbytes = Encoding.UTF8.GetBytes(txtUser);
			byte[] encryptedUser = System.Security.Cryptography.ProtectedData.Protect(userbytes, additionalEntropy, DataProtectionScope.LocalMachine);

			byte[] passbytes = Encoding.UTF8.GetBytes(txtPassword);
			byte[] encryptedPassword = System.Security.Cryptography.ProtectedData.Protect(passbytes, encryptedUser, DataProtectionScope.LocalMachine);

			encUser = ByteArrayToString(encryptedUser);
			encPassword = ByteArrayToString(encryptedPassword);
			return true;
		}

		private static bool DecryptUserCreds(string EncryptedName, string EncryptedPassword, out string UserName, out SecureString password)
		{
			password = new System.Security.SecureString();

			byte[] userEncBytes = StringToByteArray(EncryptedName);
			byte[] passEncBytes = StringToByteArray(EncryptedPassword);

			byte[] userDecBytes = ProtectedData.Unprotect(userEncBytes, additionalEntropy, DataProtectionScope.LocalMachine);
			UserName = Encoding.UTF8.GetString(userDecBytes);

			// Decrypt password with additional entropy of the encoded user name
			byte[] passDecBytes = ProtectedData.Unprotect(passEncBytes, userEncBytes, DataProtectionScope.LocalMachine);
			string tpassword = Encoding.UTF8.GetString(passDecBytes);
			foreach (var c in tpassword)
			{
				password.AppendChar(c);
			}
			return true;
		}

		private static string ByteArrayToString(byte[] ba)
		{
			return BitConverter.ToString(ba).Replace("-", "");
		}

		private static byte[] StringToByteArray(String hex)
		{
			int NumberChars = hex.Length;
			byte[] bytes = new byte[NumberChars / 2];
			for (int i = 0; i < NumberChars; i += 2)
				bytes[i / 2] = Convert.ToByte(hex.Substring(i, 2), 16);
			return bytes;
		}
	}
}
